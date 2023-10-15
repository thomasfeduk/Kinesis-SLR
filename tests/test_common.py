import unittest
from includes.common import list_append_upto_n_items_total, list_append_upto_n_items_from_new_list


class ListAppendUptoNItemsFromNewList(unittest.TestCase):
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

    def test_exception_non_numeric_string_upto(self):
        with self.assertRaises(ValueError) as ex:
            list_append_upto_n_items_from_new_list([], [], 'hello')
        self.assertIn("String value must be numeric. Passed value: <class 'str'> 'hello'", str(ex.exception))

    def test_exception_non_numeric_list_upto(self):
        with self.assertRaises(TypeError) as ex:
            list_append_upto_n_items_from_new_list([], [], object)
        self.assertIn("Value must be a numeric string, float or int. Passed value: <class 'type'> <class 'object'>",
                      str(ex.exception))

    def test_exception_non_numeric_obj_upto(self):
        with self.assertRaises(TypeError) as ex:
            list_append_upto_n_items_from_new_list([], [], [])
        self.assertIn("Value must be a numeric string, float or int. Passed value: <class 'list'> []",
                      str(ex.exception))

    def test_exception_negative_upto(self):
        with self.assertRaises(ValueError) as ex:
            list_append_upto_n_items_from_new_list([], [], -5)
        self.assertIn("Value must be 0 or greater. Passed value: <class 'int'> -5", str(ex.exception))

    def test_exception_string_base_list(self):
        with self.assertRaises(TypeError) as ex:
            list_append_upto_n_items_from_new_list('blah', [])
        self.assertIn("base_list must be a list. Passed value: <class 'str'> 'blah", str(ex.exception))

    def test_exception_string_from_list(self):
        with self.assertRaises(TypeError) as ex:
            list_append_upto_n_items_from_new_list([], 'blah2')
        self.assertIn("from_list must be a list. Passed value: <class 'str'> 'blah2", str(ex.exception))

    def test_empty_base_list_no_upto(self):
        base_list = []
        from_list = list(range(50))
        self.assertEqual(list_append_upto_n_items_from_new_list(base_list, from_list), list(range(50)))

    def test_empty_base_list_with_upto_zero(self):
        base_list = []
        from_list = list(range(5))
        self.assertEqual(list_append_upto_n_items_from_new_list(base_list, from_list, 0), [])

    def test_empty_base_list_with_upto(self):
        base_list = []
        from_list = list(range(50))
        self.assertEqual(list_append_upto_n_items_from_new_list(base_list, from_list, 25), list(range(25)))

    def test_empty_base_list_with_upto_equal_from(self):
        base_list = []
        from_list = list(range(50))
        self.assertEqual(list_append_upto_n_items_from_new_list(base_list, from_list, 50), list(range(50)))

    def test_empty_base_list_with_upto_equal_over(self):
        base_list = []
        from_list = list(range(50))
        self.assertEqual(list_append_upto_n_items_from_new_list(base_list, from_list, 55), list(range(50)))

    def test_combine_lists_full_no_upto(self):
        base_list = list(range(5))
        from_list = list(range(7))
        self.assertEqual(list_append_upto_n_items_from_new_list(base_list, from_list), [0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 5, 6])

    def test_combine_lists_full_with_within_range_upto_exact(self):
        base_list = list(range(5))
        from_list = list(range(7))
        self.assertEqual(list_append_upto_n_items_from_new_list(base_list, from_list, 12), [0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 5, 6])

    def test_combine_lists_full_with_within_range_upto_equal(self):
        base_list = list(range(5))
        from_list = list(range(15))
        self.assertEqual(list_append_upto_n_items_from_new_list(base_list, from_list, 15), [0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14])

    def test_combine_lists_full_with_exceeding_range_upto_less_than(self):
        base_list = list(range(5))
        from_list = list(range(25))
        self.assertEqual(list_append_upto_n_items_from_new_list(base_list, from_list, 15), [0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14])

    def test_combine_lists_partial(self):
        base_list = list(range(5))
        from_list = list(range(7))
        self.assertEqual(list_append_upto_n_items_from_new_list(base_list, from_list, 7), [0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 5,6])

    def test_confirm_copy_is_used(self):
        base_list = list(range(5))
        base_list_orig = base_list.copy()
        from_list = list(range(7))
        base_list_new = list_append_upto_n_items_from_new_list(base_list, from_list)
        self.assertEqual(base_list_new, [0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 5, 6])
        self.assertEqual(base_list, list(range(5)))
        self.assertEqual(base_list, base_list_orig)


class ListAppendUptoNItemsTotal(unittest.TestCase):
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

    def test_exception_non_numeric_string_upto(self):
        with self.assertRaises(ValueError) as ex:
            list_append_upto_n_items_total([], [], 'hello')
        self.assertIn("String value must be numeric. Passed value: <class 'str'> 'hello'", str(ex.exception))

    def test_exception_non_numeric_list_upto(self):
        with self.assertRaises(TypeError) as ex:
            list_append_upto_n_items_total([], [], object)
        self.assertIn("Value must be a numeric string, float or int. Passed value: <class 'type'> <class 'object'>",
                      str(ex.exception))

    def test_exception_non_numeric_obj_upto(self):
        with self.assertRaises(TypeError) as ex:
            list_append_upto_n_items_total([], [], [])
        self.assertIn("Value must be a numeric string, float or int. Passed value: <class 'list'> []",
                      str(ex.exception))

    def test_exception_negative_upto(self):
        with self.assertRaises(ValueError) as ex:
            list_append_upto_n_items_total([], [], -5)
        self.assertIn("Value must be 0 or greater. Passed value: <class 'int'> -5", str(ex.exception))

    def test_exception_string_base_list(self):
        with self.assertRaises(TypeError) as ex:
            list_append_upto_n_items_total('blah', [])
        self.assertIn("base_list must be a list. Passed value: <class 'str'> 'blah", str(ex.exception))

    def test_exception_string_from_list(self):
        with self.assertRaises(TypeError) as ex:
            list_append_upto_n_items_total([], 'blah2')
        self.assertIn("from_list must be a list. Passed value: <class 'str'> 'blah2", str(ex.exception))

    def test_empty_base_list_no_upto(self):
        base_list = []
        from_list = list(range(50))
        self.assertEqual(list_append_upto_n_items_total(base_list, from_list), list(range(50)))

    def test_empty_base_list_with_upto_zero(self):
        base_list = []
        from_list = list(range(5))
        self.assertEqual(list_append_upto_n_items_total(base_list, from_list, 0), [])

    def test_empty_base_list_with_upto(self):
        base_list = []
        from_list = list(range(50))
        self.assertEqual(list_append_upto_n_items_total(base_list, from_list, 25), list(range(25)))

    def test_empty_base_list_with_upto_equal_from(self):
        base_list = []
        from_list = list(range(50))
        self.assertEqual(list_append_upto_n_items_total(base_list, from_list, 50), list(range(50)))

    def test_empty_base_list_with_upto_equal_over(self):
        base_list = []
        from_list = list(range(50))
        self.assertEqual(list_append_upto_n_items_total(base_list, from_list, 55), list(range(50)))

    def test_combine_lists_full_no_upto(self):
        base_list = list(range(5))
        from_list = list(range(7))
        self.assertEqual(list_append_upto_n_items_total(base_list, from_list), [0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 5, 6])

    def test_combine_lists_full_with_upto_exact(self):
        base_list = list(range(5))
        from_list = list(range(7))
        self.assertEqual(list_append_upto_n_items_total(base_list, from_list, 12), [0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 5, 6])

    def test_combine_lists_full_with_upto_over(self):
        base_list = list(range(5))
        from_list = list(range(7))
        self.assertEqual(list_append_upto_n_items_total(base_list, from_list, 15), [0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 5, 6])

    def test_combine_lists_partial(self):
        base_list = list(range(5))
        from_list = list(range(7))
        self.assertEqual(list_append_upto_n_items_total(base_list, from_list, 7), [0, 1, 2, 3, 4, 0, 1])

    def test_confirm_copy_is_used(self):
        base_list = list(range(5))
        base_list_orig = base_list.copy()
        from_list = list(range(7))
        base_list_new = list_append_upto_n_items_total(base_list, from_list)
        self.assertEqual(base_list_new, [0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 5, 6])
        self.assertEqual(base_list, list(range(5)))
        self.assertEqual(base_list, base_list_orig)
