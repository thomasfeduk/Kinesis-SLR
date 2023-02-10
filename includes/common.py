from typing import Union
import os
from os import path
import json
import logging
import traceback
import yaml
from abc import ABC, abstractmethod
import datetime
import includes.exceptions as exceptions
from includes.debug import pvdd, pvd, die
from typing import Any, Callable

log = logging.getLogger()


# Abstract class that allows population of pre-defined attributes from a passed dict or json
class BaseSuperclass(ABC):
    @abstractmethod
    def __init__(self, passed_data: Union[dict, str] = None):
        # No attributes at superclass level

        # Superclass only:
        self._load_base_superclass_data(passed_data)
        self._base_superclass_passed_data = passed_data

    # This converts the passed data to a dict, then for each key that exists in the dic that is defined
    # as a mapping attribute, we set that attribute to the value of the key
    # This allows us to define the known attributes ahead of time on a per-class basis, then just throw
    # data at it, and it smartly populates the attributes if they exist without having to do it manually.
    def _load_base_superclass_data(self, passed_data: Union[dict, str]):
        if passed_data is None:
            return
        try:
            passed_data = json_or_dict_or_obj_to_dict(passed_data)
            for key in passed_data:
                # We make SURE to ensure it's not callable, so they don't try to overwrite __del__
                # or something by naming a key that is a method as opposed to a property

                # We first check if we have hidden _ prefixed versions of the values, so we can define property
                # decorators. Otherwise, look for the standard attribute names
                if hasattr(self, "_" + key) and not callable(getattr(self, "_" + key)):
                    setattr(self, "_" + key, passed_data[key])
                else:
                    if hasattr(self, key) and not callable(getattr(self, key)):
                        setattr(self, key, passed_data[key])
        except Exception as ex:
            raise ValueError(f"Could not convert passed_data to a dict.\npassed_data: {repr(passed_data)}") from ex

    # Returns a recursive dict of the entire object and any attributes that are also objects
    # Similar to .__dict__ but recursive
    def dict(self) -> dict:
        try:
            dict_dump = json.loads(
                json.dumps(self, default=lambda o: getattr(o, '__dict__', str(o)))
            )
        except Exception as ex:
            raise exceptions.InternalError(f"base_superclass: Error occurred calling .dict()") from ex
        return dict_dump

    @staticmethod
    def req_valid(func: Callable[..., Any]) -> Callable[..., Any]:
        def wrapper(*args: Union[dict, str], **kwargs) -> Any:
            if type(args[1]) not in [dict, str]:
                raise exceptions.InvalidArgumentException(f'Not a dict or str. Given: {repr(args[1])}')
            return func(*args, **kwargs)
        return wrapper


class Woof(BaseSuperclass):
    def __init__(self, data):
        self.firstname = None
        super().__init__(data)
        self.test(5)

    @BaseSuperclass.req_valid
    def test(self, data):
        pvdd(self)







class BaseCommonClass(BaseSuperclass, ABC):
    @abstractmethod
    def __init__(self, passed_data: Union[dict, str] = None):
        # Define the default attributes
        if not hasattr(self, '_proprules'):
            self._proprules = PropRules()
        # Have to call parent after defining attributes to populate them
        super().__init__(passed_data)
        self._is_valid()
        self._post_init_processing()

    def _is_valid(self):
        self._is_valid_proprules()

    # Class can use this to implement any post-init processing of properties (ie uppercaseing values,
    # setting defaults etc.)
    def _post_init_processing(self):
        del self._base_superclass_passed_data

    def _is_valid_proprules(self) -> None:
        attribs = {}
        for item in dir(self):
            if not callable(getattr(self, item)):
                attribs[item] = getattr(self, item)
        self._proprules.validate(attribs)


class RestrictedCollection(ABC):
    def __init__(self, items):
        if not isinstance(items, list):
            raise TypeError(f"Type list is expected. Received:  {type(items)} {repr(items)}")
        self._last = 0
        self._items = items
        for item in self._items:
            self._validate_item(item)

    @property
    @abstractmethod
    def expected_type(self):
        return object  # Set your allowed object type here

    def __iter__(self):
        self._last = 0
        return self

    def __next__(self):
        if self._last >= len(self._items):
            raise StopIteration
        self._last += 1
        return self._items[self._last - 1]

    def __getitem__(self, index):
        return self._items[int(index)]

    def __len__(self):
        return len(self._items)

    def _validate_item(self, value):
        if isinstance(value, self.expected_type):
            return value
        raise TypeError(f"Each item in the list must be of type {repr(self.expected_type)}. "
                        f"Received: {type(value)} {repr(value)}\nPassed data: {repr(self._items)}")


class PropRules:
    def __init__(self):
        """
        types: [{"attrib1", [int, float]}]
        numeric: ["attrib1", "attrib2"]
        numeric_positive: ["attrib1", "attrib2"]
        """

        self._types = {}
        self._numeric = []
        self._numeric_positive = []

    def add_prop(self, prop_name: str, *,
                 types=None,
                 numeric: bool = None,
                 numeric_positive: bool = None,
                 ):
        if types is None:
            types = []

        if types is not None:
            self._types[prop_name] = types

        if numeric is not None and numeric_positive is not None:
            raise exceptions.InvalidArgumentException("numeric and numeric_positive cannot both be specified.")

        if numeric is not None:
            self._numeric.append(prop_name)
        if numeric_positive is not None:
            self._numeric_positive.append(prop_name)

    def validate(self, attribs=None, orig_passed_data: str = None):
        # Drop the special attribs if set
        if "_proprules" in attribs.keys():
            del attribs["_proprules"]
        if "__dict__" in attribs.keys():
            del attribs["__dict__"]

        if attribs is None:
            attribs = {}

        debug_passed_data = ""
        if orig_passed_data is not None:
            debug_passed_data = f"\nOriginal passed data: {orig_passed_data}"

        for attrib in self._types:
            self.is_valid_types(attrib, attribs, debug_passed_data)
        for attrib in self._numeric:
            self._is_valid_numeric(attrib, attribs, debug_passed_data)
        for attrib in self._numeric_positive:
            self._is_valid_numeric_pos(attrib, attribs, debug_passed_data)

    def is_valid_types(self, attrib, attribs, debug_passed_data):
        self.is_valid_exists(attrib, attribs, debug_passed_data)
        if type(attribs[attrib]) not in self._types[attrib]:
            raise exceptions.InvalidArgumentException(
                f'"{attrib}" attribute must be of type: {str(self._types[attrib])}'
                f'\nReceived: {repr(type(attribs[attrib]))} {repr(attribs[attrib])}'
                f'{debug_passed_data}')

    def _is_valid_numeric(self, attrib, attribs, debug_passed_data):
        self.is_valid_exists(attrib, attribs, debug_passed_data)

        try:
            validate_numeric(attribs[attrib])
        except (TypeError, ValueError) as ex:
            raise exceptions.InvalidArgumentException(
                f'"{attrib}" must be a numeric value. Received: '
                f'{type(attribs[attrib])} {repr(attribs[attrib])}') from ex

    def _is_valid_numeric_pos(self, attrib, attribs, debug_passed_data):
        self.is_valid_exists(attrib, attribs, debug_passed_data)

        try:
            validate_numeric_pos(attribs[attrib])
        except (TypeError, ValueError) as ex:
            raise exceptions.InvalidArgumentException(
                f'"{attrib}" must be a positive numeric value. Received: '
                f'{type(attribs[attrib])} {repr(attribs[attrib])}') from ex

    def is_valid_exists(self, attrib, attribs, debug_passed_data):
        if attrib not in attribs.keys():
            raise exceptions.InternalError(
                f'"{attrib}" is a required attribute and does not exist in attributes list.{debug_passed_data}')


def list_append_upto_n_items(a_list: list, b_list: list, upto_item_count: int = 0):
    """
    Appends upto X items in the from_list to the a_list
    :param a_list: The list which we call the .append() method on
    :param b_list: The list which we read and append the first X items onto a_list
    :param upto_item_count: The number of items from the b_list that get added to the a_list in index order

    No return value since we just update the mutable a_list that is passed by reference
    """
    # Fresh instance, so we can return a new instance and not update by reference the original a_list
    a_list_new = a_list.copy()
    i = 0
    for item in b_list:
        if upto_item_count == 0 or i < upto_item_count:
            a_list.append(item)
        i += 1
    return a_list


def count_files_in_dir(dir_path=str) -> int:
    file_count = 0
    files = os.scandir(dir_path)
    for file in files:
        if file.is_file():
            file_count += 1
    return file_count


def validate_datetime(timestamp: str) -> str:
    try:
        datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        raise ValueError("Incorrect datetime format, should be YYYY-MM-DD HH:MM:SS. Value provided: " + repr(timestamp))
    return timestamp


def read_config(filename: str) -> dict:
    if not path.exists(filename):
        raise FileNotFoundError(f'The specified config file does not exist: {filename}')
    f = open(filename, "r")
    file_yaml_raw = f.read()
    f.close()
    yaml_data = yaml.safe_load(file_yaml_raw)
    return yaml_data


def validate_numeric(check_value: Union[str, int, float]) -> float:
    if not isinstance(check_value, str) and not isinstance(check_value, int) and not isinstance(check_value, float):
        raise TypeError('Value must be a numeric string, float or int.')
    try:
        float(check_value)
    except ValueError:
        raise ValueError('String value must be numeric.')
    return float(check_value)


def validate_numeric_pos(check_value: Union[str, int, float]) -> float:
    float_val = validate_numeric(check_value)
    if float_val < 0:
        raise ValueError('Value must be 0 or greater.')
    return float_val


# If passed a number, it returns upto max, or the input if it's less, otherwise return max as the default
def get_max_of(input_val: Union[int, str, float], max_val: Union[int, str, float]) -> float:
    input_val = validate_numeric_pos(input_val)
    max_val = validate_numeric_pos(max_val)
    if input_val <= max_val:
        return input_val
    return max_val


def json_or_dict_or_obj_to_dict(data: Union[dict, str, object]) -> dict:
    data_orig = data
    # If data is a json string, convert it to a dict
    if isinstance(data_orig, str):
        try:
            data = json.loads(data_orig)
        except Exception as ex:
            raise TypeError('Could not json loads data: ' + repr(ex))

    if isinstance(data, dict):
        return data

    # If it's not a string and not a dict, it should be an object
    try:
        data = data_orig.__dict__
    except Exception as ex:
        raise TypeError('Could not json loads data.__dict__: ' + repr(ex))

    if not isinstance(data, dict):
        raise TypeError('data should be a dict, object or json. Type given: ' +
                        repr(type(data)))

    return data


# Gets the stack trace of an exception
def get_exception_trace(ex: Exception) -> str:
    if ex is None:
        return ''
    return "".join(traceback.TracebackException.from_exception(ex).format())
