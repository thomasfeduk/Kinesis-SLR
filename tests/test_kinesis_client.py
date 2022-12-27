import json
import importlib
import unittest
import unittest.mock as mock
from unittest.mock import patch
import sys


class Main(unittest.TestCase):
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

    def test_ClientConfig(self):
        self.assertEqual(1, 2)

