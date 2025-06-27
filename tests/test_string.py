import unittest
import re


class TestStringMethods(unittest.TestCase):
    
    def normalize_string(self, str):
        # Replace sequences of space and/or hyphen with single underscore
        return re.sub(r'[\s\-]+', '_', str.strip())

    def test_normalize_string(self):
        #s = "hello   world---this  is--a   test"
        s = " "
        self.assertEqual(self.normalize_string(s), "hello_world_this_is_a_test")
        # Check that s.split fails when the separator is not a string

if __name__ == '__main__':
    unittest.main()