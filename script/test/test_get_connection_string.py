# coding=utf-8

"""
get_connection_string用テストファイル
"""

import unittest
import os
import tempfile

import sys
sys.path.append('..')
from get_connection_string import get_connection_string


class TestGetConnectionString(unittest.TestCase):
    """
    TestModule for collect_request
    """
    def test_get_connection_string(self) -> None:
        """
        get_connection_stringのテスト
        """
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        temp_file.close()
        try:
            with open(temp_file.name, 'wb') as fd:
                fd.write(b'# Comment\n#\n\nhogehoge.com:5432:name:scott:tiger')
            self.assertEqual(get_connection_string(temp_file.name),
                             'dbname=name user=scott password=tiger host=hogehoge.com port=5432')
            with open(temp_file.name, 'wb') as fd:
                fd.write(b'hogehoge.com:5432:name:scott:tiger\n\n')
            self.assertEqual(get_connection_string(temp_file.name),
                             'dbname=name user=scott password=tiger host=hogehoge.com port=5432')
        finally:
            os.remove(temp_file.name)
