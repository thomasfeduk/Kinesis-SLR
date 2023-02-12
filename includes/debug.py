import sys
from var_dump import var_dump
from collections.abc import Iterable
try:
    from enum import Enum
except ImportError:
    Enum = type(str)

try:
    from types import NoneType
except ImportError:
    NoneType = type(None)

if sys.version_info > (3,):
    long = int
    unicode = str


def pvd(data):
    # var_dump(data)
    var_dump(strip_proprules_recursively(data))


def pvdd(data):
    pvd(data)
    exit(0)


strip_proprules_recursively_i = 0
def strip_proprules_recursively(data):
    global strip_proprules_recursively_i
    strip_proprules_recursively_i += 1

    data_stripped = data

    if hasattr(data_stripped, '_proprules'):
        del data_stripped._proprules
    # if hasattr(data_stripped, '_SequenceNumber'):
    #     die('got seq')

    if hasattr(data_stripped, '__dict__'):
        for i in data_stripped.__dict__:
            print(f"abc{i}")
            print(getattr(data_stripped, i))
            setattr(data_stripped, i, strip_proprules_recursively(getattr(data_stripped, i)))

    if hasattr(data_stripped, '__iter__') and type(data_stripped) not in (tuple, list, dict, int, str, float, long, bool, NoneType, unicode):
        i = 0
        for item in data_stripped:
            print(f"xyz{item}")
            # if strip_proprules_recursively_i > 5:
            #     die('end of resurve')
            data_stripped[i] = strip_proprules_recursively(item)


    # elif hasattr(data_stripped, '__iter__'):
    #     i = 0
    #     for item in data_stripped:
    #         print(data_stripped)
    #         # die()
    #         data_stripped[i] = 1
    #         print(data_stripped)
    #         die()
    #         i += 1

    return data_stripped
# def strip_proprules_recursively(data):
#     data_stripped = data
#
#     # Strip the extensive verbose _proprules properly output by var_dump
#     try:
#         if getattr(data_stripped, '_proprules'):
#             del data_stripped._proprules
#     except AttributeError:
#         pass
#
#     try:
#         for i in data_stripped.__dict__:
#             setattr(data_stripped, i, strip_proprules_recursively(i))
#     except AttributeError:
#         pass
#     return data_stripped


def die(msg: str = None):
    if msg is not None:
        print(msg)
    exit(0)
