import os
from os import path
import json
import logging
import traceback
import yaml
from abc import ABC, abstractmethod
import datetime
from includes.debug import pvdd, pvd, die

log = logging.getLogger()


# Abstract class that allows population of pre-defined attributes from a passed dict or json
class BaseSuperclass(ABC):
    @abstractmethod
    def __init__(self, passed_data: [dict, str] = None):
        # Define the attributes for this class
        # None at superclass level

        # Superclass only:
        self._load_base_superclass_data(passed_data)

    # This converts the passed data to a dict, then for each key that exists in the dic that is defined
    # as a mapping attribute, we set that attribute to the value of the key
    # This allows us to define the known attributes ahead of time on a per-class basis, then just throw
    # data at it, and it smartly populates the attributes if they exist without having to do it manually.
    def _load_base_superclass_data(self, passed_data: [dict, str]):
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

    # Returns a recursive dict of the entire object and any attributes that are also objects
    # Similar to .__dict__ but recursive
    def dict(self) -> dict:
        try:
            dict_dump = json.loads(
                json.dumps(self, default=lambda o: getattr(o, '__dict__', str(o)))
            )
        except Exception as ex:
            raise RuntimeError(f"base_superclass: Error occurred calling .dict()") from ex
        return dict_dump


class BaseCommonClass(BaseSuperclass, ABC):
    @abstractmethod
    def __init__(self, passed_data: [dict, str] = None):
        # Have to call parent after defining attributes to populate them
        super().__init__(passed_data)
        self._is_valid()
        self._post_init_processing()

    @abstractmethod
    def _is_valid(self):
        pass

    # Class can use this to implement any post-init processing of properties (ie uppercaseing values,
    # setting defaults etc.)
    @abstractmethod
    def _post_init_processing(self):
        pass


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


def validate_numeric_pos(check_value: [str, int, float]) -> float:
    if not isinstance(check_value, str) and not isinstance(check_value, int) and not isinstance(check_value, float):
        raise TypeError('Value must be a numeric string, float or int.')
    try:
        float(check_value)
    except ValueError:
        raise ValueError('String value must be numeric.')

    if float(check_value) < 0:
        raise ValueError('Value must be 0 or greater.')
    return float(check_value)


# If passed a number, it returns upto max, or the input if it's less, otherwise return max as the default
def get_max_of(input_val: [int, str, float], max_val: [int, str, float]) -> float:
    input_val = validate_numeric_pos(input_val)
    max_val = validate_numeric_pos(max_val)
    if input_val <= max_val:
        return input_val
    return max_val


def json_or_dict_or_obj_to_dict(data: [dict, str, object]) -> dict:
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
