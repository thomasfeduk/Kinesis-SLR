from os import path
import json
import logging
import traceback
import yaml
from debug import pvdd, pvd, die

log = logging.getLogger()


def read_config(filename: str) -> str:
    if not path.exists(filename):
        raise FileNotFoundError(f'The specified config file does not exist: {filename}')
    f = open(filename, "r")
    manifest_yaml = f.read()
    f.close()
    yaml_data = yaml.safe_load(manifest_yaml)
    config_json = json.dumps(yaml_data)
    return config_json


def validate_numeric(check_value: str or int) -> int:
    """ Confirms the id is either a string or int

    Arguments:
    id (str or int): The number to check

    Returns:
    int: int version of the number

    Raises:
    TypeError: If wrong data type
    ValueError: If not numeric

    """
    if not isinstance(check_value, str) and not isinstance(check_value, int):
        raise TypeError('Value must be a numeric string or int.')
    if isinstance(check_value, str) and not check_value.isnumeric():
        raise ValueError('String value must be numeric.')
    return int(check_value)


def json_or_dict_or_obj_to_dict(data: [dict, str, object]):
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
def get_exception_trace(ex):
    if ex is None:
        return ''
    return "".join(traceback.TracebackException.from_exception(ex).format())


# Abstract class, extend only
class BaseSuperclass:
    def __init__(self, passed_data: [dict, str] = None):
        # Define the attributes for this class
        # None at superclass level

        # Superclass only:
        self.load(passed_data)

    # This converts the passed data to a dict, then for each key that exists in the dic that is defined
    # as a mapping attribute, we set that attribute to the value of the key
    # This allows us to define the known attributes ahead of time on a per-class basis, then just throw
    # data at it, and it smartly populates the attributes if they exist without having to do it manually.
    def load(self, passed_data):
        if passed_data is None:
            return

        try:
            passed_data = json_or_dict_or_obj_to_dict(passed_data)

            for key in passed_data:
                # We make SURE to ensure it's not callable so they don't try to overwrite __del__
                # or something by naming a key that is a method as opposed to a property
                if hasattr(self, key) and not callable(getattr(self, key)):
                    setattr(self, key, passed_data[key])
        except Exception as ex:
            log.warning(__name__ + ".load(): Could not convert passed_data to a dict " + repr(ex) +
                        "passed_data: " + repr(passed_data))

    # Returns a recursive dict of the entire object and any attributes that are also objects
    # Similar to .__dict__ but recursive
    def dict(self):
        try:
            dict_dump = json.loads(
                json.dumps(self, default=lambda o: getattr(o, '__dict__', str(o)))
            )
        except Exception as ex:
            log.error('base_superclass: Error occurred calling .dict(). Error: '
                      + repr(ex)
                      + "\nStacktrace:\n" + get_exception_trace(ex))
            return {}
        return dict_dump


class ConfigSLR(BaseSuperclass):
    def __init__(self, passed_data: [dict, str] = None):
        # Have to call parent after defining attributes other they are not populated
        super().__init__(passed_data)
        self.is_valid()

    # Overriden by the extended class
    def is_valid(self):
        raise NotImplementedError('The is_valid() method must be overwritten by extended classes')
