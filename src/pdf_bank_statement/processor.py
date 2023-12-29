from .__import import *

class BankTransactionProcesser():
    schema = {
        'Date': np.dtype('<M8[ns]'),
        'Description': np.dtype('O'),
        'Type': np.dtype('O'),
        'Money In (£)': np.dtype('float64'),
        'Money Out (£)': np.dtype('float64'),
        'Balance (£)': np.dtype('float64')
        }
    
    def __init__(self, 
                 rowsep = r'(?!\d)\.(?<!\d)',
                 table_chunk_pattern = r'(?<=Your Transactions\n)(.*?)(?=\(Continued on next page\)|Transaction types)'
                 ):
        
        # Class Parameters
        self.rowsep = rowsep
        self.table_chunk_pattern = table_chunk_pattern
        # Cacheing Varaibles
        self.full_text = '' # Cacheing Original For Debugging
        self.extracted_records = {}  # Initialize an empty dictionary
        self.dfs = []  # Initialize an empty list to store parsed DataFrames
        self.pdf_file_path = '' # File Name iis used to 
    
    def reset_tmp_vars(self):
        '''
        Certain Varaible here are reserved to accumulate values so we can perform bulk actions.
        '''
        self.dfs = []
        self.extracted_records = {}

    def _parse_transaction(self, transaction: str):
        '''taken a fraction of transaction'''
        transaction = transaction.strip('\n')
        parts = transaction.split('\n', 1)
        
        if len(parts) == 2:
            key, value = parts
            key = key.strip()
            value = value.strip()
            
            current_column = '<init>'

            if key == 'Column':
                current_column = value
                self.extracted_records[current_column] = []
            else:
                current_column = key
                self.extracted_records[current_column].append(value)
        else: 
            # raise Exception('Failed to extract parts to 2 length')
            pass

    def read(self, pdf_file_path, reset = True):
        '''This method will save every read into dfs'''

        # clean cached dataframe fragment
        self.dfs = [] if reset else self.dfs

        full_text = self.extract_text_from_pdf(pdf_file_path)
        transaction_blocks = self.extract_transaction_blocks(full_text)

        self.process_transactions(transaction_blocks)

        concatenated_df = self.get_concatenated_dataframe()
        dt_converted = self.convert_dtype(concatenated_df)

        if reset: 
            return(dt_converted)
        else: 
            print(f'Read additional {len(transaction_blocks)} tables.Total {len(self.dfs)} tables populated.')
    
    def extract_text_from_pdf(self, pdf_file_path):
        self.pdf_file_path = pdf_file_path
        text = ""
        with open(pdf_file_path, 'rb') as readerOBJ:
            pdfReader = PyPDF2.PdfReader(readerOBJ)
            for page in pdfReader.pages:
                text += page.extract_text() + "\n"
        self.full_text = text
        return text

    def extract_transaction_blocks(self, text: str):
        '''
        This field process all text in each pages are return part they think should be
        the table content of this bankstatement;
        '''
        # Use the provided regular expression pattern to extract 'transaction_blocks' from the input 'text'
        pattern = self.table_chunk_pattern
        transaction_blocks = re.findall(pattern, text, re.DOTALL)
        return transaction_blocks

    def process_transactions(self, transaction_blocks: List[str]):
        '''
        This field process 
        '''
        for transaction_block in transaction_blocks:
            transaction_block = transaction_block.strip('\n')
            transaction_list = re.split(self.rowsep, transaction_block)

            for transaction in transaction_list:
                self._parse_transaction(transaction)

            df = pd.DataFrame(self.extracted_records)
            df['src_file'] = self.pdf_file_path
            self.dfs.append(df)
            self.extracted_records = {}  # Reset extracted_records for the next chunk

    def get_concatenated_dataframe(self):
        if not self.dfs:
            raise ValueError("No dataframes have been processed. Call process_transactions() first.")
        else:
            concatenated_df = pd.concat(self.dfs, ignore_index=True)
            self.reset_tmp_vars()
        return concatenated_df
    
    def convert_dtype(self, df: pd.DataFrame):
        '''Convert Certain Type to Numeric and Parse Date Time'''
        # Create Two Funcions to Clean Numeric Date Time
        def clean_numbers(df, col):
            df[col] = df[col].apply(
                lambda x: x.str.replace(',', '').str.replace('blank','0')
            ).astype(float)
            return(df)
        def parse_datetime(df, col):
            df[col] = pd.to_datetime(df[col], format='%d %b %y')
            return(df)
        
        # A pipe perhaps is more memory efficient. 
        (df
            .pipe(clean_numbers, ['Money In (£)', 'Money Out (£)', 'Balance (£)'])
            .pipe(parse_datetime, 'Date'))
        print('cleaning complete.')
        return df
        