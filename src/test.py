import os
import duckdb
# assert os.listdir('resources')==['Statement_2023_9.pdf']
print(os.getcwd())

test_file_path ='package/src/resources/Statement_2023_9.pdf'


from pdf_bank_statement import reader as bank_reader


df = bank_reader.read(test_file_path)

with duckdb.connect() as con:
    print(con.sql("select * from df order by random() limit 3"))

print(df.dtypes)


from pdf_bank_statement import analyser

analysed_data = analyser.analyse(df)

print(analysed_data)

# with duckdb.connect() as con:
#     # print(con.sql("select * from analysed_data order by random() limit 3"))
#     print(analysed_data.head(3))