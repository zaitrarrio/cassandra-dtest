# Tools for creating and verifying data
import cPickle
import re
from cassandra.concurrent import execute_concurrent_with_args

from cPickle import HIGHEST_PROTOCOL


def strip(val):
    # remove spaces and pipes from beginning/end
    return val.strip().strip('|')


def parse_headers_into_list(data):
    # throw out leading/trailing space and pipes
    # so we can split on the data without getting
    # extra empty fields
    rows = map(strip, data.split('\n'))

    # remove any remaining empty lines (i.e. '') from data
    rows = filter(None, rows)

    # separate headers from actual data and remove extra spaces from them
    headers = [unicode(h.strip()) for h in rows.pop(0).split('|')]
    return headers


def get_row_multiplier(row):
    # find prefix like *1234 meaning create 1,234 rows
    row_cells = [l.strip() for l in row.split('|')]
    m = re.findall('\*(\d+)$', row_cells[0])

    if m:
        return int(m[0])

    return None


def row_has_multiplier(row):
    if get_row_multiplier(row) is not None:
        return True

    return False


def parse_row_into_dict(row, headers, format_funcs=None):
    row_cells = [l.strip() for l in row.split('|')]

    if row_has_multiplier(row):
        row_multiplier = get_row_multiplier(row)
        row = '|'.join(row_cells[1:])  # cram remainder of row back into foo|bar format

        for i in xrange(row_multiplier):
            yield parse_row_into_dict(row, headers, format_funcs=format_funcs)
    else:
        row_map = dict(zip(headers, row_cells))

        if format_funcs:
            for colname, value in row_map.items():
                func = format_funcs.get(colname)

                if func is not None:
                    row_map[colname] = func(value)

        yield row_map


def parse_data_into_dicts(data, format_funcs=None):
    # throw out leading/trailing space and pipes
    # so we can split on the data without getting
    # extra empty fields
    rows = map(strip, data.split('\n'))

    # remove any remaining empty lines (i.e. '') from data
    rows = filter(None, rows)

    # remove headers
    headers = parse_headers_into_list(rows.pop(0))

    for row in rows:
        if row_has_multiplier(row):
            for r in parse_row_into_dict(row, headers, format_funcs=format_funcs):
                yield r
        else:
            yield parse_row_into_dict(row, headers, format_funcs=format_funcs)


def create_rows(log, data, cursor, table_name, format_funcs=None, prefix='', postfix=''):
    """
    Creates db rows using given cursor, with table name provided,
    using data formatted like:

    |colname1|colname2|
    |value2  |value2  |

    format_funcs should be a dictionary of {columnname: function} if data needs to be formatted
    before being included in CQL.
    """
    prepared = None
    current_chunk = []
    count = 0

    for gens in parse_data_into_dicts(data, format_funcs=format_funcs):
        for row_dict in gens:
            count += 1

            mill, remainder = divmod(count, 1000000)
            if remainder == 0:
                print "{} million rows created".format(mill)

            # prepare if this is the first statement
            if prepared is None:
                prepared = cursor.prepare("{prefix} INSERT INTO {table} ({cols}) values ({vals}) {postfix}".format(
                    prefix=prefix, table=table_name, cols=', '.join(row_dict.keys()),
                    vals=', '.join('?' for k in row_dict.keys()), postfix=postfix)
                )

            current_chunk.append(row_dict)

            if len(current_chunk) > 1000:
                current_chunk_values = [r.values() for r in current_chunk]

                for i, (status, result) in enumerate(execute_concurrent_with_args(cursor, prepared, current_chunk_values)):
                    log.append(current_chunk[i])

                # reset for building the next chunk
                current_chunk = []

    # write any remaining rows that are part of a last chunk
    if len(current_chunk) > 0:
        current_chunk_values = [r.values() for r in current_chunk]

        for i, (status, result) in enumerate(execute_concurrent_with_args(cursor, prepared, current_chunk_values)):
            log.append(current_chunk[i])

    print("create_rows complete")
    log.mark_complete()


def flatten_into_set(iterable):
    # use flatten() then convert to a set for set comparisons
    return set(flatten(iterable))


def flatten(list_of_dicts):
    # flatten list of dicts into list of strings for easier comparison
    # and easier set membership testing (e.g. foo is subset of bar)
    flattened = []

    for _dict in list_of_dicts:
        sorted_keys = sorted(_dict)
        items = ['{}__{}'.format(k, _dict[k]) for k in sorted_keys]
        flattened.append('__'.join(items))

    return flattened


class InMemoryCassLog(list):
    _complete = False

    def mark_complete(self):
        self._complete = True

    def is_complete(self):
        return self._complete


class OnDiskCassLog(object):
    _complete = False
    _filename = None
    _fh_append = None

    def __init__(self, filename=None):
        if filename is None:
            self._filename = 'casslogfile.txt'
        else:
            self._filename = filename

        # fh only for appending
        self._fh_append = open(self._filename, 'ab')

    def append(self, val):
        cPickle.dump(val, self._fh_append, HIGHEST_PROTOCOL)

    def mark_complete(self):
        self._fh_append.flush()
        self._fh_append.close()
        self._complete = True

    def is_complete(self):
        return self._complete

    def step(self):
        with open(self._filename, 'rb') as fh:
            while True:
                try:
                    yield cPickle.load(fh)
                except EOFError:
                    break
