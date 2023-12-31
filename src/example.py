


# This is just a simple scipt run to see if everyting is working properly=
# This is strictly an integration test
import os
import duckdb
import unittest

print(os.getcwd())
test_file_path ='resources/lloyd_bank_sample.pdf'
# Test Integration
from pdf_bank_statement import reader as bank_reader

# Test Read --------------
df1 = bank_reader.read(test_file_path) # read a single file
df2 = bank_reader.read([test_file_path,test_file_path]) # read multiple file
print(duckdb.from_df(df1.head(3)))
print(duckdb.from_df(df2.head(3)))

assert df2.shape[0] == 2 * df1.shape[0]
assert df1.shape[0] == 134

# Test Analysing -----------
from pdf_bank_statement import analyser

analysed = analyser.analyse(df1)
print("Preview analysed data frame")
print(duckdb.from_df(analysed.data.head(3)))


if __name__ == '__main__':
    unittest.main()