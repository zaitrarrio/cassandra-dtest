import pprint


def debug(*args):
    for arg in args:
        print pprint.pformat(arg),
    print
