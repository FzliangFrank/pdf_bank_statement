# pdf_bank_statement

Particularly for extracting lloyd bank statement in the uk.



```python
import pdf_bank_statement.reader as reader
df = reader.read(all_statement)
```

```python
import pdf_bank_statement.analyser as analyser
categorised = analyser.analyse(df)
```
