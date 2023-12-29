from .__import import *
from .processor import BankTransactionProcesser


def read(pdf_file_path, processor=None):
    """
    Read bankstatement on the fly. This is considered as interface 
    for bank statement read. 
    
    Processor arugment should let you swap engines in case your statment or
    function is different.
    """
    
    if processor is None:
        processor = BankTransactionProcesser()
    else:
        processor = processor()
    
    if isinstance(pdf_file_path, (list)):

        for file in pdf_file_path:
            processor.read(file, reset=False)

        df = processor.get_concatenated_dataframe()
    else:   
        df = processor.read(pdf_file_path)
    return df
        
