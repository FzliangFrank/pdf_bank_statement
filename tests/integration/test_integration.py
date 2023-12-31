

import os
import duckdb
import unittest

import pdf_bank_statement.reader as bank_reader
from pathlib import Path


class TestRead(unittest.TestCase):
    
    def setUp(self):
        self.test_file_path ='fixtures/lloyd_bank_sample.pdf'
    def test_read_single(self):
        assert Path(self.test_file_path).exists()
        df = bank_reader.read(self.test_file_path) # read a single file
        self.assertListEqual(
            df.columns,
            ['Date', 'Description', 'Type', 'Money In (£)', 'Money Out (£)','Balance (£)', 'Category', 'src_file']
        )

    def test_read_multiple(self):
        df = bank_reader.read([self.test_file_path, self.test_file_path])


if __name__ == '__main__':
    unittest.main()