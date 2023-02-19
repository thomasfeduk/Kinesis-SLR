# A quick and dirty debug script to aid in development to get a visual minified resursive text dump of an object's
# contents No error handling or proper  form in this file. It is a quick hack as it is exclusively used as a simple
# reference/view during dev to check values

import copy
import sys
from var_dump import var_dump

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
    var_dump(strip_proprules_recursively(data))


def pvdd(data):
    pvd(data)
    exit(0)


# Blindly with a hammer deletes from a deep copy all occurrences of _proprules
# from any object/type to be used in var_dump so we dont clutter the output
def strip_proprules_recursively(data):
    data_stripped = copy.deepcopy(data)

    try:
        del data_stripped._proprules
    except Exception:
        pass

    try:
        i = -1
        for item in data_stripped:
            i += 1
            try:
                del data_stripped[i]._proprules
            except Exception:
                pass
            try:
                del data_stripped[item]._proprules
            except Exception:
                pass
    except Exception:
        pass

    if hasattr(data_stripped, '__dict__'):
        for i in data_stripped.__dict__:
            try:
                delattr(data_stripped, '_proprules')
            except Exception:
                pass
            try:
                del data_stripped[i]._proprules
            except Exception:
                pass

            setattr(data_stripped, i, strip_proprules_recursively(getattr(data_stripped, i)))

    if hasattr(data_stripped, '__iter__') and type(data_stripped) not in (
            tuple, int, str, float, long, bool, NoneType, unicode):
        i = 0
        try:
            data_stripped_new = data_stripped.copy()
        except Exception:
            data_stripped_new = data_stripped
        for item in data_stripped:
            try:
                delattr(item, '_proprules')
            except Exception:
                pass
            try:
                delattr(data_stripped_new, '_proprules')
            except Exception:
                pass
            try:
                del data_stripped_new[i]._proprules
            except Exception:
                pass
            data_stripped_new[i] = strip_proprules_recursively(item)
            i += 1

    return data_stripped


def die(msg: str = None):
    if msg is not None:
        print(msg)
    exit(0)
