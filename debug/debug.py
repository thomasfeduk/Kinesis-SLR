# A quick and dirty debug script to aid in development as-needed to get a visual minified recursive text dump of an
# object's contents. It is never used in the project's code itself. No error handling or proper formatting in this
# file, ignore the ugliness. It is a quick hack I use exclusively as a simple reference/view during dev to check
# values in a nested set of unknown values at virtually any depth

import copy
import os
import sys
from io import StringIO
from var_dump import var_dump, var_export
import json
import jsonpickle
import inspect

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


class _readoutputbuffer(list):
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self

    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        del self._stringio  # free up some memory
        sys.stdout = self._stdout


def jout(data):
    print(json.dumps(data, indent=4, default=str))


def joutd(data):
    jout(data)
    die()


def pvd(data):
    var_dump(_strip_proprules_recursively(data))


def pvde(data):
    return var_export(_strip_proprules_recursively(data))


def pvdd(data):
    pvd(data)
    exit(0)


def pvdfile(filename: str, data, *, overwrite: bool = False):
    directory = "debug"
    if not os.path.exists(directory):
        os.mkdir(directory)

    if len(filename) < 1:
        raise ValueError(f'Filename must be at least one character.')
    try:
        mode = "x"
        if overwrite:
            mode = "w"
        with open(f"{directory}/debug-{filename}.dump", mode) as f:
            f.write(jsonpickle.dumps(data, indent=4, make_refs=False))
    except FileExistsError as ex:
        raise FileExistsError(f'The debug output file "{filename}" already exists.') from ex


def pvddfile(filename, data, *, overwrite: bool = False):
    pvdfile(filename, data, overwrite=overwrite)
    die()


def called_from_where():
    frame = inspect.currentframe().f_back
    filename = frame.f_code.co_filename
    line_number = frame.f_lineno
    print(f"Called from {filename} line {line_number}")


# Blindly and with the power of Thor's hammer, recursively delete from a deep copy all occurrences of _proprules
# from any object/type to be used in var_dump, so we don't clutter the output
def _strip_proprules_recursively(data):
    try:
        data_stripped = copy.deepcopy(data)
    except Exception:
        data_stripped = data

    try:
        del data_stripped._proprules
    except Exception:
        pass
    try:
        delattr(data_stripped, '_proprules')
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
                delattr(data_stripped[i], '_proprules')
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
                delattr(data_stripped[i], '_proprules')
            except Exception:
                pass
            try:
                del data_stripped[i]._proprules
            except Exception:
                pass

            setattr(data_stripped, i, _strip_proprules_recursively(getattr(data_stripped, i)))

    if hasattr(data_stripped, '__iter__') and type(data_stripped) not in (
            tuple, int, str, float, long, bool, NoneType, unicode):
        i = 0
        try:
            data_stripped_new = data_stripped.copy()
        except Exception:
            data_stripped_new = data_stripped
        try:
            for item in data_stripped:
                try:
                    delattr(data_stripped_new[i], '_proprules')
                except Exception:
                    pass
                try:
                    delattr(data_stripped_new[item], '_proprules')
                except Exception:
                    pass
                try:
                    del data_stripped_new[i]._proprules
                except Exception:
                    pass
                try:
                    data_stripped_new[i] = _strip_proprules_recursively(item)
                except Exception:
                    pass
                i += 1
                try:
                    data_stripped = data_stripped_new.copy()
                except Exception:
                    data_stripped = data_stripped_new
        except Exception:
                pass
    return data_stripped


def die(msg: str = None):
    if msg is not None:
        print(msg)
    exit(0)
