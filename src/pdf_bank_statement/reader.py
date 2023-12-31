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
            processor.process(file)

        df = processor.export()
    else:   
        df = processor.process(pdf_file_path).export()
    return df
        
