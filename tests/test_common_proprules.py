import unittest
import unittest.mock as mock
from unittest.mock import patch
import includes.common as common
import includes.exceptions as exceptions


class PropRules(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_invalid_numeric_true(self):
        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", numeric=True)
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)

        data = {
            "attrib_invalid": "abc"
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' is a required attribute and does not exist in attributes received. "
                      "Original passed data: None", str(ex.exception))

        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self.attrib_invalid = None
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", numeric=True)
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)

        data = {
            "attrib_invalid": "abc"
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' must be a numeric value. Received: <class 'str'> 'abc'", str(ex.exception))

        data = {
            "attrib_invalid": "231a"
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' must be a numeric value. Received: <class 'str'> '231a'", str(ex.exception))

        data = {
            "attrib_invalid": None
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' must be a numeric value. Received: <class 'NoneType'> None", str(ex.exception))

        data = {
            "attrib_invalid": []
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' must be a numeric value. Received: <class 'list'> []", str(ex.exception))

        data = {
            "attrib_invalid": {}
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' must be a numeric value. Received: <class 'dict'> {}", str(ex.exception))

        data = {
            "attrib_invalid": True
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' must be a numeric value. Received: <class 'bool'> True", str(ex.exception))

        data = {
            "attrib_invalid": False
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' must be a numeric value. Received: <class 'bool'> False", str(ex.exception))


    def test_invalid_numeric_false(self):
        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", numeric=False)
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)

        data = {
            "attrib_invalid": "abc"
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' is a required attribute and does not exist in attributes received. "
                      "Original passed data: None", str(ex.exception))

        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self.attrib_invalid = None
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", numeric=False)
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)


        data = {"attrib_invalid": "0"}
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' cannot be a numeric value. Received: <class 'str'> '0'", str(ex.exception))

        data = {"attrib_invalid": "-5"}
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' cannot be a numeric value. Received: <class 'str'> '-5'", str(ex.exception))

        data = {"attrib_invalid": "5"}
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' cannot be a numeric value. Received: <class 'str'> '5'", str(ex.exception))

        data = {"attrib_invalid": 5}
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' cannot be a numeric value. Received: <class 'int'> 5", str(ex.exception))

        data = {"attrib_invalid": -2}
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' cannot be a numeric value. Received: <class 'int'> -2", str(ex.exception))

        data = {"attrib_invalid": 1.235}
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' cannot be a numeric value. Received: <class 'float'> 1.235", str(ex.exception))

        data = {"attrib_invalid": -1.25}
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' cannot be a numeric value. Received: <class 'float'> -1.25", str(ex.exception))


        data = {"attrib_invalid": 0.0}
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' cannot be a numeric value. Received: <class 'float'> 0.0", str(ex.exception))

        data = {"attrib_invalid": 0}
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' cannot be a numeric value. Received: <class 'int'> 0", str(ex.exception))

    def test_valid_numeric_true(self):
        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self.attrib_invalid = None
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", numeric=True)
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)

        data = {"attrib_invalid": "0"}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": "1"}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": "-1"}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": "1.5"}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": "-2.5"}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": 0}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": 1}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": -1}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": 1.5}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": -2.5}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

    def test_valid_numeric_false(self):
        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self.attrib_invalid = None
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", numeric=False)
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)

        data = {"attrib_invalid": False}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": True}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": None}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": {}}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": []}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": ""}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

    def test_invalid_numeric_pos_true(self):
        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", numeric_positive=True)
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)

        data = {
            "attrib_invalid": "abc"
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' is a required attribute and does not exist in attributes received. "
                      "Original passed data: None", str(ex.exception))

        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self.attrib_invalid = None
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", numeric_positive=True)
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)

        data = {
            "attrib_invalid": "abc"
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' must be a positive numeric value. Received: <class 'str'> 'abc'", str(ex.exception))

        data = {
            "attrib_invalid": "231a"
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' must be a positive numeric value. Received: <class 'str'> '231a'", str(ex.exception))

        data = {
            "attrib_invalid": None
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' must be a positive numeric value. Received: <class 'NoneType'> None", str(ex.exception))

        data = {
            "attrib_invalid": []
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' must be a positive numeric value. Received: <class 'list'> []", str(ex.exception))

        data = {
            "attrib_invalid": {}
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' must be a positive numeric value. Received: <class 'dict'> {}", str(ex.exception))

        data = {
            "attrib_invalid": True
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' must be a positive numeric value. Received: <class 'bool'> True", str(ex.exception))

        data = {
            "attrib_invalid": False
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' must be a positive numeric value. Received: <class 'bool'> False", str(ex.exception))

        data = {
            "attrib_invalid": "-2"
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' must be a positive numeric value. Received: <class 'str'> '-2'", str(ex.exception))

        data = {
            "attrib_invalid": "-5"
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' must be a positive numeric value. Received: <class 'str'> '-5'", str(ex.exception))

        data = {
            "attrib_invalid": "-1.2"
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' must be a positive numeric value. Received: <class 'str'> '-1.2'", str(ex.exception))


        data = {
            "attrib_invalid": -5
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' must be a positive numeric value. Received: <class 'int'> -5", str(ex.exception))

        data = {
            "attrib_invalid": -1.2
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' must be a positive numeric value. Received: <class 'float'> -1.2", str(ex.exception))

    def test_invalid_numeric_pos_false(self):
        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", numeric_positive=False)
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)

        data = {
            "attrib_invalid": "abc"
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' is a required attribute and does not exist in attributes received. "
                      "Original passed data: None", str(ex.exception))

        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self.attrib_invalid = None
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", numeric_positive=False)
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)


        data = {"attrib_invalid": "0"}
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' cannot be a positive numeric value. Received: <class 'str'> '0'", str(ex.exception))

        data = {"attrib_invalid": "5"}
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' cannot be a positive numeric value. Received: <class 'str'> '5'", str(ex.exception))

        data = {"attrib_invalid": 5}
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' cannot be a positive numeric value. Received: <class 'int'> 5", str(ex.exception))

        data = {"attrib_invalid": 1.235}
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' cannot be a positive numeric value. Received: <class 'float'> 1.235", str(ex.exception))

        data = {"attrib_invalid": 0.0}
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' cannot be a positive numeric value. Received: <class 'float'> 0.0", str(ex.exception))

        data = {"attrib_invalid": 0}
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' cannot be a positive numeric value. Received: <class 'int'> 0", str(ex.exception))

    def test_valid_numeric_pos_true(self):
        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self.attrib_invalid = None
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", numeric_positive=True)
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)

        data = {"attrib_invalid": "0"}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": "1"}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": "1.5"}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": 0}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": 1}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": 1.5}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

    def test_valid_numeric_pos_false(self):
        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self.attrib_invalid = None
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", numeric_positive=False)
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)

        data = {"attrib_invalid": False}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": True}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": None}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": {}}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": []}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": ""}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": "-1"}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": "-2.5"}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": -1}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": -2.5}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

    def test_invalid_populated_true(self):
        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", populated=True)
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)

        data = {
            "attrib_invalid": "abc"
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' is a required attribute and does not exist in attributes received. "
                      "Original passed data: None", str(ex.exception))

        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self.attrib_invalid = None
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", populated=True)
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)

        data = {
            "attrib_invalid": ""
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' is a required attribute that must be populated (empty collections or "
                      "whitespace characters are not considered populated). Received: <class 'str'> ''",
                      str(ex.exception))

        data = {
            "attrib_invalid": " "
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' is a required attribute that must be populated (empty collections or "
                      "whitespace characters are not considered populated). Received: <class 'str'> ' '",
                      str(ex.exception))

        data = {
            "attrib_invalid": "\t"
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' is a required attribute that must be populated (empty collections or "
                      "whitespace characters are not considered populated). Received: <class 'str'> '\\t'",
                      str(ex.exception))

        data = {
            "attrib_invalid": "\n"
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' is a required attribute that must be populated (empty collections or "
                      "whitespace characters are not considered populated). Received: <class 'str'> '\\n'",
                      str(ex.exception))

        data = {
            "attrib_invalid": None
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' is a required attribute that must be populated (empty collections or "
                      "whitespace characters are not considered populated). Received: <class 'NoneType'> None",
                      str(ex.exception))

        data = {
            "attrib_invalid": []
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' is a required attribute that must be populated (empty collections or "
                      "whitespace characters are not considered populated). Received: <class 'list'> []",
                      str(ex.exception))

        data = {
            "attrib_invalid": {}
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' is a required attribute that must be populated (empty collections or "
                      "whitespace characters are not considered populated). Received: <class 'dict'> {}",
                      str(ex.exception))

    def test_invalid_populated_false(self):
        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", populated=False)
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)

        data = {
            "attrib_invalid": "abc"
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' is a required attribute and does not exist in attributes received. "
                      "Original passed data: None", str(ex.exception))

        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self.attrib_invalid = None
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", populated=False)
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)

        data = {
            "attrib_invalid": " "
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' is a required attribute but must be null, an empty string"
                      ", or empty collection. "
                      "Received: <class 'str'> ' '", str(ex.exception))

        data = {
            "attrib_invalid": "a"
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' is a required attribute but must be null, an empty string"
                      ", or empty collection. "
                      "Received: <class 'str'> 'a'",
                      str(ex.exception))

        data = {
            "attrib_invalid": "\n"
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' is a required attribute but must be null, an empty string"
                      ", or empty collection. "
                      "Received: <class 'str'> '\\n'",
                      str(ex.exception))

        data = {
            "attrib_invalid": "\t"
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' is a required attribute but must be null, an empty string"
                      ", or empty collection. "
                      "Received: <class 'str'> '\\t'",
                      str(ex.exception))

        data = {
            "attrib_invalid": True
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' is a required attribute but must be null, an empty string"
                      ", or empty collection. "
                      "Received: <class 'bool'> True",
                      str(ex.exception))

        data = {
            "attrib_invalid": False
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' is a required attribute but must be null, an empty string"
                      ", or empty collection. "
                      "Received: <class 'bool'> False",
                      str(ex.exception))

        data = {
            "attrib_invalid": [None]
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' is a required attribute but must be null, an empty string"
                      ", or empty collection. "
                      "Received: <class 'list'> [None]",
                      str(ex.exception))

        data = {
            "attrib_invalid": {"None": None}
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' is a required attribute but must be null, an empty string"
                      ", or empty collection. "
                      "Received: <class 'dict'> {'None': None}",
                      str(ex.exception))

    def test_valid_populated_false(self):
        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self.attrib_invalid = None
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", populated=False)
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)

        data = {"attrib_invalid": None}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": {}}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": []}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": ""}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

    def test_valid_populated_true(self):
        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self.attrib_invalid = None
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", populated=True)
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)

        data = {"attrib_invalid": "a"}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": {"item": None}}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": [None]}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": True}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": False}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": 5}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": -5}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": "-5"}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": -5.2}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": "-5.3"}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": 0}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": 0.0}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": "0.0"}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

    def test_invalid_regex(self):
        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", regexp=r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)

        data = {
            "attrib_invalid": "abc"
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' is a required attribute and does not exist in attributes received. "
                      "Original passed data: None", str(ex.exception))

        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self.attrib_invalid = None
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", regexp=r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)

        data = {
            "attrib_invalid": ""
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' must be a string and match the pattern: "
                      "'\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z'. "
                      "Received: <class 'str'> ''", str(ex.exception))

        data = {
            "attrib_invalid": "02023-04-30T12:34:56Z"
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' must be a string and match the pattern: "
                      "'\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z'. "
                      "Received: <class 'str'> '02023-04-30T12:34:56Z'", str(ex.exception))

        data = {
            "attrib_invalid": " "
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' must be a string and match the pattern: "
                      "'\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z'. "
                      "Received: <class 'str'> ' '", str(ex.exception))

        data = {
            "attrib_invalid": []
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' must be a string and match the pattern: "
                      "'\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z'. "
                      "Received: <class 'list'> []", str(ex.exception))

        data = {
            "attrib_invalid": {}
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' must be a string and match the pattern: "
                      "'\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z'. "
                      "Received: <class 'dict'> {}", str(ex.exception))

        data = {
            "attrib_invalid": None
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' must be a string and match the pattern: "
                      "'\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z'. "
                      "Received: <class 'NoneType'> None", str(ex.exception))

        data = {
            "attrib_invalid": True
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' must be a string and match the pattern: "
                      "'\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z'. "
                      "Received: <class 'bool'> True", str(ex.exception))

        data = {
            "attrib_invalid": False
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' must be a string and match the pattern: "
                      "'\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z'. "
                      "Received: <class 'bool'> False", str(ex.exception))

    def test_valid_regex(self):
        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self.attrib_invalid = None
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", regexp=r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)

        data = {"attrib_invalid": "2023-04-30T12:34:56Z"}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

    def test_invalid_types(self):
        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", types=[str])
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)

        data = {
            "attrib_invalid": "abc"
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' is a required attribute and does not exist in attributes received. "
                      "Original passed data: None", str(ex.exception))

        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self.attrib_invalid = None
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", types=[str])
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)

        data = {
            "attrib_invalid": []
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' attribute must be of type: [<class 'str'>]. "
                      "Received:<class 'list'> []", str(ex.exception))

        def DynamicClass2(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self.attrib_invalid = None
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", types=[list])
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass2(common.BaseCommonClass)

        data = {
            "attrib_invalid": {}
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' attribute must be of type: [<class 'list'>]. "
                      "Received:<class 'dict'> {}", str(ex.exception))

        DynamicClass = DynamicClass2(common.BaseCommonClass)

        data = {
            "attrib_invalid": {"item": "one"}
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' attribute must be of type: [<class 'list'>]. "
                      "Received:<class 'dict'> {'item': 'one'}", str(ex.exception))


        def DynamicClass3(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self.attrib_invalid = None
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", types=[list, dict, bool])
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass3(common.BaseCommonClass)

        data = {
            "attrib_invalid": None
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' attribute must be of type: [<class 'list'>, <class 'dict'>, <class 'bool'>]. "
                      "Received:<class 'NoneType'> None", str(ex.exception))

        data = {
            "attrib_invalid": "hello"
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' attribute must be of type: [<class 'list'>, <class 'dict'>, <class 'bool'>]. "
                      "Received:<class 'str'> 'hello", str(ex.exception))

    def test_invalid_enums(self):
        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", enums=["UPDATE"])
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)

        data = {
            "attrib_invalid": "abc"
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' is a required attribute and does not exist in attributes received. "
                      "Original passed data: None", str(ex.exception))

        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self.attrib_invalid = None
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", enums=["UPDATE"])
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)

        data = {
            "attrib_invalid": ""
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' attribute must be of the following string enums: ['UPDATE']. "
                      "Received:<class 'str'> ''", str(ex.exception))

        data = {
            "attrib_invalid": "update"
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' attribute must be of the following string enums: ['UPDATE']. "
                      "Received:<class 'str'> 'update'", str(ex.exception))

        data = {
            "attrib_invalid": "UPDATED"
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' attribute must be of the following string enums: ['UPDATE']. "
                      "Received:<class 'str'> 'UPDATED'", str(ex.exception))

        data = {
            "attrib_invalid": []
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' attribute must be of the following string enums: ['UPDATE']. "
                      "Received:<class 'list'> []", str(ex.exception))

        def DynamicClass2(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self.attrib_invalid = None
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", enums=["UPDATE"])
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass2(common.BaseCommonClass)

        data = {
            "attrib_invalid": {}
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' attribute must be of the following string enums: ['UPDATE']. "
                      "Received:<class 'dict'> {}", str(ex.exception))

        DynamicClass = DynamicClass2(common.BaseCommonClass)

        data = {
            "attrib_invalid": {"item": "one"}
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' attribute must be of the following string enums: ['UPDATE']. "
                      "Received:<class 'dict'> {'item': 'one'}", str(ex.exception))

        def DynamicClass3(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self.attrib_invalid = None
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", enums=["UPDATE", "CREATED"])
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass3(common.BaseCommonClass)

        data = {
            "attrib_invalid": None
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' attribute must be of the following string enums: ['UPDATE', 'CREATED']. "
                      "Received:<class 'NoneType'> None", str(ex.exception))

        data = {
            "attrib_invalid": "hello"
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' attribute must be of the following string enums: ['UPDATE', 'CREATED']. "
                      "Received:<class 'str'> 'hello", str(ex.exception))

        def DynamicClass3(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self.attrib_invalid = None
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", enums=["5", 10])
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass3(common.BaseCommonClass)

        data = {
            "attrib_invalid": 5
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' attribute must be of the following string enums: ['5', 10]. "
                      "Received:<class 'int'> 5", str(ex.exception))

        data = {
            "attrib_invalid": "10"
        }
        with self.assertRaises(exceptions.ValidationError) as ex:
            DynamicClass(data)
        self.assertIn("'attrib_invalid' attribute must be of the following string enums: ['5', 10]. "
                      "Received:<class 'str'> '10'", str(ex.exception))

    def test_valid_enums(self):
        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self.attrib_invalid = None
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", enums=["CREATED"])
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)

        data = {"attrib_invalid": "CREATED"}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        def DynamicClass2(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self.attrib_invalid = None
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", enums=["CREATED", "UPDATED", "5", 10])
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass2(common.BaseCommonClass)

        data = {"attrib_invalid": "CREATED"}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": "UPDATED"}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": "5"}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": 10}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

    def test_valid_types(self):
        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self.attrib_invalid = None
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", types=[str])
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)

        data = {"attrib_invalid": "2023-04-30T12:34:56Z"}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        def DynamicClass2(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self.attrib_invalid = None
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_invalid", types=[bool, float, dict])
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass2(common.BaseCommonClass)

        data = {"attrib_invalid": False}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": 1.23}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": {}}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

        data = {"attrib_invalid": {"item": "two"}}
        self.assertIsInstance(DynamicClass(data), DynamicClass)

    def test_valid_empty(self):
        data = {}

        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self._proprules = common.PropRules()
                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)
        self.assertIsInstance(DynamicClass(data), DynamicClass)

    def test_valid_full_all_permutations(self):
        data = {
            "attrib_no_rule": None,
            "attrib_numeric": "5",
            "attrib_not_numeric": "hello",
            "attrib_numeric_positive_int": 10,
            "attrib_numeric_positive_float": 15.123,
            "attrib_not_numeric_positive_neg_int": -5,
            "attrib_not_numeric_positive_neg_str": "",
            "attrib_not_numeric_positive_neg_dict": {},
            "attrib_not_numeric_positive_neg_list": [],
            "attrib_not_numeric_positive_neg_float": -0.1,
            "attrib_not_numeric_positive_neg_true": True,
            "attrib_not_numeric_positive_neg_false": False,
            "attrib_populated_list": ['item'],
            "attrib_populated_dict1": {'item': "thing"},
            "attrib_populated_dict2": {'0': "False"},
            "attrib_populated_dict3": {'0': None},
            "attrib_not_populated_list": [],
            "attrib_not_populated_dict": {},
            "attrib_not_populated_str": "",
            "attrib_regexp": "2023-04-30T12:34:56Z",
            "attrib_enums_int": 30,
            "attrib_enums_str": "CREATED",
            "attrib_enums_str_num": "25",
            "attrib_types_str": "value_here1",
            "attrib_types_str_int": "value_here2",
            "attrib_types_int": 3,
            "attrib_types_list": [],
            "attrib_types_dict": {},
            "attrib_types_float": 5.5,
            "attrib_types_bool_true": True,
            "attrib_types_bool_false": False,
            "attrib_extra_item": "extra_item",
        }

        def DynamicClass(superclass):
            class _DynamicClass(superclass):
                def __init__(self, data):
                    self.attrib_no_rule = None
                    self.attrib_numeric = None
                    self.attrib_not_numeric = None
                    self.attrib_numeric_positive_int = None
                    self.attrib_numeric_positive_float = None
                    self.attrib_not_numeric_positive_neg_int = None
                    self.attrib_not_numeric_positive_neg_str = None
                    self.attrib_not_numeric_positive_neg_dict = None
                    self.attrib_not_numeric_positive_neg_list = None
                    self.attrib_not_numeric_positive_neg_float = None
                    self.attrib_not_numeric_positive_neg_true = None
                    self.attrib_not_numeric_positive_neg_false = None
                    self.attrib_populated_list = None
                    self.attrib_populated_dict1 = None
                    self.attrib_populated_dict2 = None
                    self.attrib_populated_dict3 = None
                    self.attrib_not_populated_list = None
                    self.attrib_not_populated_dict = None
                    self.attrib_not_populated_str = None
                    self.attrib_regexp = None
                    self.attrib_enums_int = None,
                    self.attrib_enums_str = None,
                    self.attrib_enums_str_num = None,
                    self.attrib_types_str = None
                    self.attrib_types_str_int = None
                    self.attrib_types_int = None
                    self.attrib_types_list = None
                    self.attrib_types_dict = None
                    self.attrib_types_float = None
                    self.attrib_types_bool_true = None
                    self.attrib_types_bool_false = None
                    self._proprules = common.PropRules()
                    self._proprules.add_prop("attrib_numeric", numeric=True)
                    self._proprules.add_prop("attrib_not_numeric", numeric=False)
                    self._proprules.add_prop("attrib_numeric_positive_int", numeric_positive=True)
                    self._proprules.add_prop("attrib_numeric_positive_float", numeric_positive=True)
                    self._proprules.add_prop("attrib_not_numeric_positive_neg_int", numeric_positive=False)
                    self._proprules.add_prop("attrib_not_numeric_positive_neg_str", numeric_positive=False)
                    self._proprules.add_prop("attrib_not_numeric_positive_neg_dict", numeric_positive=False)
                    self._proprules.add_prop("attrib_not_numeric_positive_neg_list", numeric_positive=False)
                    self._proprules.add_prop("attrib_not_numeric_positive_neg_float", numeric_positive=False)
                    self._proprules.add_prop("attrib_not_numeric_positive_neg_true", numeric_positive=False)
                    self._proprules.add_prop("attrib_not_numeric_positive_neg_false", numeric_positive=False)
                    self._proprules.add_prop("attrib_populated_list", populated=True)
                    self._proprules.add_prop("attrib_populated_dict1", populated=True)
                    self._proprules.add_prop("attrib_populated_dict2", populated=True)
                    self._proprules.add_prop("attrib_populated_dict3", populated=True)
                    self._proprules.add_prop("attrib_not_populated_list", populated=False)
                    self._proprules.add_prop("attrib_not_populated_dict", populated=False)
                    self._proprules.add_prop("attrib_not_populated_str", populated=False)
                    self._proprules.add_prop("attrib_regexp", regexp=r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")
                    self._proprules.add_prop("attrib_enums_int", enums=[30])
                    self._proprules.add_prop("attrib_enums_str", enums=["CREATED", "UPDATE"])
                    self._proprules.add_prop("attrib_enums_str_num", enums=["25", 10, "15"])
                    self._proprules.add_prop("attrib_types_str", types=[str])
                    self._proprules.add_prop("attrib_types_str_int", types=[str, int])
                    self._proprules.add_prop("attrib_types_int", types=[int])
                    self._proprules.add_prop("attrib_types_list", types=[list])
                    self._proprules.add_prop("attrib_types_dict", types=[dict])
                    self._proprules.add_prop("attrib_types_float", types=[float])
                    self._proprules.add_prop("attrib_types_bool_true", types=[bool])
                    self._proprules.add_prop("attrib_types_bool_false", types=[bool])

                    super().__init__(data)

            return _DynamicClass

        DynamicClass = DynamicClass(common.BaseCommonClass)
        self.assertIsInstance(DynamicClass(data), DynamicClass)
