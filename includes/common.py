from __future__ import annotations
from debug.debug import *
from typing import Union, Optional
import os
import re
from os import path
import json
import logging
import traceback
import yaml
from abc import ABC, abstractmethod
import datetime
import includes.exceptions as exceptions
import struct
from typing import Type

log = logging.getLogger()


# Abstract class that allows population of pre-defined attributes from a passed dict or json
class BaseSuperclass(ABC):
    @abstractmethod
    def __init__(self, passed_data: dict | str = None):
        # No attributes at superclass level

        # Superclass only:
        self._load_base_superclass_data(passed_data)
        self._base_superclass_passed_data = passed_data

    # This converts the passed data to a dict, then for each key that exists in the dic that is defined
    # as a mapping attribute, we set that attribute to the value of the key
    # This allows us to define the known attributes ahead of time on a per-class basis, then just throw
    # data at it, and it smartly populates the attributes if they exist without having to do it manually.
    def _load_base_superclass_data(self, passed_data: dict | str):
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
            raise ValueError(f"Could not convert passed_data to a dict. passed_data: {repr(passed_data)}") from ex


class BaseCommonClass(BaseSuperclass):
    # TODO: Add decorator here to require a dict/str or exception and pass in a dict
    @abstractmethod
    def __init__(self, passed_data: dict | str = None):
        # Define the default attributes
        if not hasattr(self, '_proprules'):
            self._proprules = PropRules()
        # Have to call parent after defining attributes to populate them
        super().__init__(passed_data)
        self._is_valid()
        self._post_init_processing()

    def _is_valid(self):
        self._is_valid_proprules()

    def _is_valid_proprules(self) -> None:
        attribs = {}
        for item in dir(self):
            if not callable(getattr(self, item)):
                attribs[item] = getattr(self, item)
        self._proprules.validate(attribs)
    def _post_init_processing(self):
        del self._base_superclass_passed_data

    def __str__(self):
        attribs = []
        for item in dir(self):
            if callable(getattr(self, item)) or item[0:1] == '_':
                continue
            attribs.append(item)

        attrib_pairs = []
        for item in attribs:
            attrib_pairs.append(f"{item}={getattr(self, item)}")

        return ','.join(map(str, attrib_pairs))

    def __repr__(self):
        attribs = []
        for item in dir(self):
            if callable(getattr(self, item)) or item[0:1] == '_':
                continue
            attribs.append(item)

        attrib_pairs = []
        for item in attribs:
            attrib_pairs.append(f"{item}={repr(getattr(self, item))}")

        return ','.join(map(str, attrib_pairs))

    def __eq__(self, other):
        return repr(self) == repr(other)


class Collection:
    def __init__(self, items: list):
        self._current_index = 0
        if not isinstance(items, list):
            raise TypeError(f'Type "list" is expected. Received:  {type(items)} {repr(items)}')
        self._items = items

    def __iter__(self) -> Collection:
        self._current_index = 0
        return self

    def __next__(self):
        if self._current_index >= len(self._items):
            raise StopIteration
        self._current_index += 1
        return self._items[self._current_index - 1]

    def __getitem__(self, index):
        if isinstance(index, slice):
            start, stop, step = index.indices(len(self._items))
            return [self._items[i] for i in range(start, stop, step)]
        else:
            return self._items[int(index)]

    def __setitem__(self, key, value) -> None:
        self._items[key] = value

    def __add__(self, value: list | Collection) -> Collection:
        if isinstance(value, Collection):
            combined = self._items + value._items
        elif isinstance(value, list):
            combined = self._items + value
        else:
            raise TypeError(f"Cannot concatenate '{self.__class__.__name__}' and '{value.__class__.__name__}' objects")
        # Return a new instance
        return Collection(combined)

    def __len__(self) -> int:
        return len(self._items)

    def __str__(self) -> str:
        return str(self._items)

    def __repr__(self) -> str:
        output = f"{self.__class__.__name__}["
        output += ','.join(map(repr, self._items))
        output += f']'
        return output

    def __eq__(self, other):
        return repr(self) == repr(other)

    def append(self, item) -> None:
        self._items.append(item)

    def toJson(self, *, indent: Optional[Union[int, None]] = None) -> str:
        return json.dumps([json.loads(i.toJson()) for i in self._items], indent=indent)


class RestrictedCollection(Collection):
    def __init__(self, items):
        super().__init__(items)
        for item in self._items:
            self._validate_item(item)

    def __setitem__(self, key, value):
        self._validate_item(value)
        self._items[key] = value

    @property
    @abstractmethod
    def expected_type(self):
        return object  # Set your allowed object type here

    def _validate_item(self, value):
        if isinstance(value, self.expected_type):
            return value
        raise TypeError(f"Each item in the collection must be of type {repr(self.expected_type)}. Received: "
                        f"{type(value)} {repr(value)}")


class PropRules:
    def __init__(self):
        self._enums: dict = {}
        self._types: dict = {}
        self._numeric: dict = {}
        self._numeric_positive: dict = {}
        self._populated: dict = {}
        self._regexp: dict = {}

    def add_prop(self, prop_name: str,
                 enums: list | None = None,
                 types: list | None = None,
                 numeric: bool = None,
                 numeric_positive: bool | None = None,
                 populated: bool | None = None,
                 regexp: str | None = None,
                 ):
        """
        Add a new property to the object validation rules with the specified name and optional validation criteria.

        :param enums: (Optional[Iterable[str]]) A list of acceptable str enums.

        :param prop_name: (str): The name of the property to add.

        :param types: (Optional[Iterable[type]]): A list of types that the property value must be an instance of and
            must exist.
            If not specified, any type of value is allowed unless another rule adds a condition.

        :param numeric: (Optional[bool]): If True, the property value must exist and be a numeric type
            (int, float, etc.).
            If False, the property value must not be a numeric type.
            If not specified, any type of value is allowed unless another rule adds a condition.

        :param numeric_positive: (Optional[bool]): If True, the property value must exist and be a positive numeric type
            (int, float, etc.). Note: 0 is counted as a positive value.
            If False, the property value must not be a positive numeric type. Example: A negative numeric value or
            string value will be permitted.
            If not specified, any type of value is allowed unless another rule adds a condition.

        :param populated: (Optional[bool]): If True, the property must exist, and it's value must not be None,
            an empty string, or an empty collection.
            If False, the property must exist, and it's value must be None, an empty string, or an empty collection.
            If not specified, any value is allowed unless another rule adds a condition.

        :param regexp: (Optional[str]): A regular expression pattern that the property value must match.
            If not specified, any value is allowed unless another rule adds a condition.
        """

        if enums is not None:
            self._enums[prop_name] = enums

        if types is not None:
            self._types[prop_name] = types

        if populated is not None:
            self._populated[prop_name] = populated

        if regexp is not None:
            self._regexp[prop_name] = regexp

        if numeric is not None:
            self._numeric[prop_name] = numeric

        if numeric_positive is not None:
            self._numeric_positive[prop_name] = numeric_positive

    def validate(self, attribs=None, orig_passed_data: str = None):
        # Drop the special attribs if set
        if "_proprules" in attribs.keys():
            del attribs["_proprules"]
        if "__dict__" in attribs.keys():
            del attribs["__dict__"]

        if attribs is None:
            attribs = {}

        debug_passed_data = f" Original passed data: {repr(orig_passed_data)}"

        for attrib in self._populated:
            if self._populated[attrib] is False:
                self._is_valid_not_populated(attrib, attribs, debug_passed_data)
            else:
                self._is_valid_populated(attrib, attribs, debug_passed_data)
        for attrib in self._enums:
            self.is_valid_enums(attrib, attribs, debug_passed_data)
        for attrib in self._types:
            self.is_valid_types(attrib, attribs, debug_passed_data)
        for attrib in self._numeric:
            self._is_valid_numeric(attrib, attribs, debug_passed_data)
        for attrib in self._numeric_positive:
            self._is_valid_numeric_positive(attrib, attribs, debug_passed_data)
        for attrib in self._regexp:
            self._is_valid_regexp(attrib, attribs, debug_passed_data)

    def is_valid_enums(self, attrib, attribs, debug_passed_data):
        self._is_valid_exists(attrib, attribs, debug_passed_data)
        if attribs[attrib] not in self._enums[attrib]:
            raise exceptions.ValidationError(
                f"'{attrib}' attribute must be of the following string enums: {str(self._enums[attrib])}. Received:"
                f"{repr(type(attribs[attrib]))} {repr(attribs[attrib])}")

    def is_valid_types(self, attrib, attribs, debug_passed_data):
        self._is_valid_exists(attrib, attribs, debug_passed_data)
        if type(attribs[attrib]) not in self._types[attrib]:
            raise exceptions.ValidationError(
                f"'{attrib}' attribute must be of type: {str(self._types[attrib])}. Received:"
                f"{repr(type(attribs[attrib]))} {repr(attribs[attrib])}")

    def _is_valid_numeric(self, attrib, attribs, debug_passed_data):
        self._is_valid_exists(attrib, attribs, debug_passed_data)
        error_msg_not_numeric = f"'{attrib}' must be a numeric value. " \
                                f"Received: {type(attribs[attrib])} {repr(attribs[attrib])}"

        # If is numeric for this attrib is true, require a numeric value. If false, ensure its not numeric
        if self._numeric[attrib]:
            # Bools are not considered numeric for our PropRules
            if attribs[attrib] is True or attribs[attrib] is False:
                raise exceptions.ValidationError(error_msg_not_numeric)

            try:
                validate_numeric(attribs[attrib])
            except (TypeError, ValueError) as ex:
                raise exceptions.ValidationError(error_msg_not_numeric) from ex
            return

        # Bools are not considered numeric for our PropRules
        if attribs[attrib] is True or attribs[attrib] is False:
            return

        try:
            validate_numeric(attribs[attrib])
            raise exceptions.ValidationError(
                f"'{attrib}' cannot be a numeric value. Received: {type(attribs[attrib])} {repr(attribs[attrib])}")
        except (TypeError, ValueError) as ex:
            return

    def _is_valid_numeric_positive(self, attrib, attribs, debug_passed_data):
        self._is_valid_exists(attrib, attribs, debug_passed_data)
        error_msg_not_numeric = f"'{attrib}' must be a positive numeric value. " \
                                f"Received: {type(attribs[attrib])} {repr(attribs[attrib])}"
        # If is numeric for this attrib is true, require a numeric value. If false, ensure its not numeric
        if self._numeric_positive[attrib]:
            # Bools are not considered numeric for our PropRules,
            if attribs[attrib] is True or attribs[attrib] is False:
                raise exceptions.ValidationError(error_msg_not_numeric)
            try:
                validate_numeric_pos(attribs[attrib])
            except (TypeError, ValueError) as ex:
                raise exceptions.ValidationError(error_msg_not_numeric) from ex
            return

        # Bools are not considered numeric for our PropRules
        if attribs[attrib] is True or attribs[attrib] is False:
            return

        try:
            validate_numeric_pos(attribs[attrib])
            raise exceptions.ValidationError(
                f"'{attrib}' cannot be a positive numeric value. "
                f"Received: {type(attribs[attrib])} {repr(attribs[attrib])}")
        except (TypeError, ValueError) as ex:
            return

    def _is_valid_not_populated(self, attrib, attribs, debug_passed_data):
        self._is_valid_exists(attrib, attribs, debug_passed_data)

        if attribs[attrib] is None:
            return

        if isinstance(attribs[attrib], str) and attribs[attrib] == "":
            return

        if isinstance(attribs[attrib], (dict, list)) and len(attribs[attrib]) == 0:
            return

        raise exceptions.ValidationError(
            f"'{attrib}' is a required attribute but must be null, an empty string, "
            f"or empty collection. Received: {type(attribs[attrib])} {repr(attribs[attrib])}")

    def _is_valid_populated(self, attrib, attribs, debug_passed_data):
        self._is_valid_exists(attrib, attribs, debug_passed_data)

        if isinstance(attribs[attrib], str) and attribs[attrib].strip() != "":
            return

        if isinstance(attribs[attrib], (int, float)):
            return

        if isinstance(attribs[attrib], (dict, list)) and len(attribs[attrib]) > 0:
            return

        raise exceptions.ValidationError(
            f"'{attrib}' is a required attribute that must be populated (empty collections or whitespace characters are"
            f" not considered populated). Received: {type(attribs[attrib])} {repr(attribs[attrib])}")

    def _is_valid_regexp(self, attrib, attribs, debug_passed_data):
        self._is_valid_exists(attrib, attribs, debug_passed_data)

        pattern = self._regexp[attrib]
        error_msg_not_pattern = f"'{attrib}' must be a string and match the pattern: '{pattern}'. Received: " \
                                f"{type(attribs[attrib])} {repr(attribs[attrib])}"

        if not isinstance(attribs[attrib], str):
            raise exceptions.ValidationError(error_msg_not_pattern)

        if not re.match(pattern, attribs[attrib]):
            raise exceptions.ValidationError(error_msg_not_pattern)

    def _is_valid_exists(self, attrib, attribs, debug_passed_data):
        if attrib not in attribs.keys():
            raise exceptions.ValidationError(
                f"'{attrib}' is a required attribute and does not exist in attributes received.{debug_passed_data}")


def validate_list_append_upto_n_items_inputs(base_list: list, from_list: list, upto_item_count=None):
    if upto_item_count is not None:
        validate_numeric_pos(upto_item_count)

    if not isinstance(base_list, list):
        raise TypeError(f'base_list must be a list. Passed value: {type(base_list)} {repr(base_list)}')

    if not isinstance(from_list, list):
        raise TypeError(f'from_list must be a list. Passed value: {type(from_list)} {repr(from_list)}')


def list_append_upto_n_items_from_new_list(base_list: list, from_list: list, upto_item_count=None):
    """
    Appends upto X items in the from_list to the base_list regardless of the size of baselist
    If upto_item_count = None, always append everything
    If upto_item_count = 0, the original list will be returned with nothing new added
    If the base_list item count is => than the upto_item_count, we return just the base_list with nothing added
    :param base_list: The list which we call the .append() method on
    :param from_list: The list which we read and append the first X items onto a_list
    :param upto_item_count: The number of items from the b_list that get added to the a_list in index order
    """

    validate_list_append_upto_n_items_inputs(base_list, from_list, upto_item_count)

    # Fresh instance, so we can return a new instance and not update by reference the original a_list
    if upto_item_count is not None and len(base_list) >= upto_item_count:
        return base_list.copy()

    base_list_new = base_list.copy()
    i = 0
    for item in from_list:
        if upto_item_count is None or (upto_item_count is not None and i < upto_item_count):
            base_list_new.append(item)
        i += 1
    return base_list_new


def get_class_attribs_non_callable(obj: object) -> dict:
    attribs = {}
    for item in dir(obj):
        if not callable(getattr(obj, item)):
            attribs[item] = getattr(obj, item)
        return attribs
def list_append_upto_n_items_total(base_list: list, from_list: list, upto_item_count=None):
    """
    Appends upto X items in the from_list to the base_list but ensure the base_list never exceeds the upto_item_count
    (If base_list exceeds the upto item count we just return it unmodified)
    If upto_item_count = None, always append everything
    If upto_item_count = 0, the original list will be returned with nothing new added
    (even though it may still have more than upto_item_count items)
    If the base_list item count is => than the upto_item_count, we return just the base_list with nothing added
    """

    validate_list_append_upto_n_items_inputs(base_list, from_list, upto_item_count)

    # Fresh instance, so we can return a new instance and not update by reference the original a_list
    if upto_item_count is not None and len(base_list) >= upto_item_count:
        return base_list.copy()

    base_list_new = base_list.copy()
    for item in from_list:
        if upto_item_count is None or (upto_item_count is not None and len(base_list_new) < upto_item_count):
            base_list_new.append(item)
    return base_list_new


def require_instance(given_object: object, expected_instance_type: Type,
                     exception_type: Type[Exception] | TypeError = None):
    if not isinstance(given_object, expected_instance_type):
        raise exception_type(
            f"Instance of type {expected_instance_type} expected. Received: "
            f"{type(given_object)} {repr(given_object)}")


def require_type(given_object: object, expected_type: Type,
                 exception_type: Type[Exception] | TypeError = None):
    if type(given_object) != expected_type:
        raise exception_type(
            f"Type {expected_type} expected. Received: {type(given_object)} {repr(given_object)}")


def type_repr(input: object) -> str:
    return f"{type(input)} {repr(input)}"


def format_size(size_bytes):
    # Define suffixes and their corresponding units
    suffixes = ["B", "KB", "MB", "GB"]
    base = 1024

    # Determine the appropriate suffix and scale the size accordingly
    for i, suffix in enumerate(suffixes):
        if size_bytes < base ** (i + 1):
            size = size_bytes / base ** i
            return f"{size:.2f} {suffix}"

    # If the size is very large, use the largest suffix
    size = size_bytes / base ** (len(suffixes) - 1)
    return f"{size:.2f} {suffixes[-1]}"


def to_bytes(s):
    if type(s) is bytes:
        return s
    if type(s) is str:
        return s.encode('utf-8')
    if type(s) in [int, float]:
        return struct.pack('i', s)

    raise TypeError(f"Expected bytes, string, int or float. Value provided: {type(s)} {repr(s)}")


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


def validate_numeric(check_value: str | int | float) -> float:
    if not isinstance(check_value, (str, int, float)):
        raise TypeError(
            f'Value must be a numeric string, float or int. Passed value: {type(check_value)} {repr(check_value)}')
    try:
        float(check_value)
    except ValueError as ex:
        raise ValueError(f'String value must be numeric. Passed value: {type(check_value)} {repr(check_value)}') from ex
    return float(check_value)


def validate_numeric_pos(check_value: str | int | float) -> float:
    float_val = validate_numeric(check_value)
    if float_val < 0:
        raise ValueError(f'Value must be 0 or greater. Passed value: {type(check_value)} {repr(check_value)}')
    return float_val


# If passed a number, it returns upto max, or the input if it's less, otherwise return max as the default
def get_max_of(input_val: int | str | float, max_val: int | str | float) -> float:
    input_val = validate_numeric_pos(input_val)
    max_val = validate_numeric_pos(max_val)
    if input_val <= max_val:
        return input_val
    return max_val


def json_or_dict_or_obj_to_dict(data: dict | str | object) -> dict:
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
