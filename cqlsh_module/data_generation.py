import uuid

NUM_DYNAMIC_COLUMNS = 10
NUM_ROWS=5

saved_uuids = {}
def generate_value(row_index, col_index, data_type, prefix='value'):
    if data_type == 'ascii' or data_type == 'text':
        return prefix + str(row_index) + '_' + str(col_index)
    elif data_type == 'bigint':
        return 2**8*int(row_index)*2**16*int(col_index)
    elif data_type == 'blob':
        return generate_value(row_index, col_index, 'ascii').encode('hex')
    elif data_type == 'boolean':
        return (row_index + col_index) % 2 == 0
    elif data_type == 'decimal' or data_type == 'float' or data_type == 'double':
        return generate_value(row_index, col_index, 'int') * 1.0
    elif data_type == 'int':
        return row_index - col_index # get some negatives in there too
    elif data_type == 'varint':
        return 2**100*int(row_index)*2**200*int(col_index)
    elif data_type == 'uuid':
        key = str(row_index) + '_' + str(col_index)
        if key in saved_uuids:
            return saved_uuids[key]
        else:
            uu = str(uuid.uuid4())
            saved_uuids[key] = uu
            return uu
    elif data_type == 'varchar':
        return unicode(generate_value(row_index, col_index, 'ascii'))
    else:
        raise Exception('Invalid data type: ' + str(data_type))

        
def generate_col_name(row_index, col_index, data_type):
    return generate_value(row_index, col_index, data_type, prefix='col_name')

def generate_row_key(row_index, key_validation_class):
    return generate_value(row_index, 0, key_validation_class, prefix='key_name')
    

"""
Operations we need to permutate over:
 - batch (insert the first 5 as a batch with consistency_level=ONE. Then the
        next 5 with cl=TWO... through all consistency levels)
 - insert (insert the rest individually of the rows individually. cycle through
        consistency levels.)
 - read and validate
    the operations happen in these stages:
    1. insert
    2. update
    3. delete
    5. read and validate
    6. truncate
    7. try to read
    - read this way:
        - select * where key = <key value>
        - select column
        - select a row that doesn't exist
        - select a col that doesn't exist
        - select where col_name >= startkey
        - select where col_name <= endkey
        - select where col_name >= startkey AND col_name <= endkey
        - key in ()
        - key in () LIMIT N
        - first n
        - reversed
        - select count(1)
        - select count(*)
        - index read
    - To support this, a function should be created that can return all the
      expected data 
"""

def generate_cf_data(key_validation_class, validation, static_columns, 
    has_updated=False, has_deleted=False):
    """
    generates ALL the data that should be found currently in the column family.
    Some things change the data that is present, specifically the short-expiring ttl
    will remove some, the update will change some values, and the delete will remove
    some rows and columns.

    validation is "ascii", "int", .... This is the type of columns that dynamic columns
    will be.
    static_columns is a list of 2-tuples (<column_name>, <column_type>). The list can be empty,
    in which case a dynamic column family is assumed.

        - update (col_num==1 get updated to have a value
                generated like so: generate_value(row_index, col_index+1, data_type)
        - delete
            - row (row_num==1 will be deleted)
            - columns (col_num==4 will be deleted)
    """
#    import ipdb; ipdb.set_trace()
    insert_rows = _generate_inserted_rows(key_validation_class, validation, static_columns)


    # Make modifications to the list of what is expected.
    row_num = 0
    for row in insert_rows:
        columns = row['columns']
        if has_updated:
            col = columns[1]
            # notice that the col_num passed in is different then originally.
            new_value = generate_value(row_num, 2, col['type'])
            col['value'] = new_value
        if has_deleted:
            col_name = columns[4]['name']
            del(columns[4])
            del(row['column_dict'][col_name])
        row_num += 1

    if has_deleted:
        del(insert_rows[1])
            
    return insert_rows


def _generate_inserted_rows(key_validation_class, validation, static_columns):
    """
    generates all the rows that should be initially inserted.

    returns a list of rows, where each row is a dict with keys
    'key', 'columns', 'column_dict'. Each column in the 'columns' list is
    a dict with keys 'name', 'value', and 'type'

    Each entry in column_dict is keyed off the column name, and references
    the same dict as the column list. This dict is included because the order
    of columns can change when it is sorted by the database.

    """
    rows = []
    for row_num in xrange(NUM_ROWS):
        row_key = generate_row_key(row_num, key_validation_class)
        # make sure we have column names and types
        if static_columns:
            cols = static_columns
        else:
            cols = []
            num_cols = 2 if validation == 'boolean' else NUM_DYNAMIC_COLUMNS
            for col_num in xrange(num_cols):
                cf_name = generate_col_name(row_num, col_num, 
                        validation)
                cols.append((cf_name, validation))

        # now generate data for each column
        column_list = []
        column_dict = {}
        col_num = 0
        for col_name, col_type in cols:
            col_value = generate_value(row_num, col_num, col_type, prefix='value')
            packaged = {'name': col_name, 'value': col_value, 'type': col_type}
            column_list.append(packaged)
            column_dict[col_name] = packaged
            col_num += 1
        
        rows.append({'key': row_key, 'key_type': key_validation_class, 'columns': column_list, 'column_dict': column_dict})

    return rows

