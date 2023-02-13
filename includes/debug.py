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
    var_dump(strip_proprules_recursively(data))


def pvdd(data):
    pvd(data)
    exit(0)


def strip_proprules_recursively(data):
    data_stripped = data

    if hasattr(data_stripped, '_proprules'):
        del data_stripped._proprules

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
            setattr(data_stripped, i, strip_proprules_recursively(getattr(data_stripped, i)))

    if hasattr(data_stripped, '__iter__') and type(data_stripped) not in (
            tuple, list, dict, int, str, float, long, bool, NoneType, unicode):
        i = 0
        for item in data_stripped:
            try:
                del data_stripped[i]._proprules
            except AttributeError:
                pass
            data_stripped[i] = strip_proprules_recursively(item)

    return data_stripped


def die(msg: str = None):
    if msg is not None:
        print(msg)
    exit(0)
