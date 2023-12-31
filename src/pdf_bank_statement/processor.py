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
        self.__extracted_records = {}  # when reading lines temporarly store this into
        self.extracted_dfs = []  # a basket to store dumped data frame
        self.pdf_file_path = '' # File Name iis used to 
    
    def reset_tmp_vars(self):
        '''
        Certain Varaible here are reserved to accumulate values so we can perform bulk actions.
        '''
        self.extracted_dfs = []
        # self.__extracted_records = {}


    def process(self, pdf_file_path):
        '''This method will save every read into dfs'''
        if Path(pdf_file_path).exists():
            pass
        else:
            raise FileNotFoundError
        
        self.pdf_file_path = pdf_file_path

        full_text = self.extract_text_from_pdf(pdf_file_path);self.full_text = full_text
        table_contents = self.detect_table_contents(full_text)
        extracted_df = self.parse_dataframe(table_contents)

        self.extracted_dfs.append(extracted_df)
        
        print(f'Read... Total {len(self.extracted_dfs)} tables populated.')
        return self
    
    def export(self):
        data = pd.concat(self.extracted_dfs, ignore_index=True)
        data = self.convert_dtype(data)
        self.extracted_dfs = []
        return data
    
    def extract_text_from_pdf(self, pdf_file_path):
        
        text = ""
        with open(pdf_file_path, 'rb') as readerOBJ:
            pdfReader = PyPDF2.PdfReader(readerOBJ)
            for page in pdfReader.pages:
                text += page.extract_text() + "\n"
        
        return text

    def detect_table_contents(self, text: str):
        '''
        This field process all text in each pages are return part they think should be
        the table content of this bankstatement;
        '''
        # Use the provided regular expression pattern to extract 'table_contents' from the input 'text'
        pattern = self.table_chunk_pattern
        table_contents = re.findall(pattern, text, re.DOTALL)
        return table_contents
    
    def __parse_and_append_one_record(self, transaction: str):
        '''taken a fraction of transaction'''
        transaction = transaction.strip('\n')
        parts = transaction.split('\n', 1)
        
        if len(parts) == 2:
            key, value = parts
            key = key.strip() # trim space
            value = value.strip()
            
            current_column = '<init>'

            if key == 'Column':
                # if key is column extract this part and return a 
                current_column = value
                self.__extracted_records[current_column] = []
            else:
                current_column = key
                self.__extracted_records[current_column].append(value)
        else: 
            # raise Exception('Failed to extract parts to 2 length')
            pass
        return self
    
    def parse_dataframe(self, pages: List[str]):
        '''
        This field process 
        '''

        final_dataframe = pd.DataFrame()
        for page, table_content in enumerate(pages):
            table_content = table_content.strip('\n')
            lines_txt = re.split(self.rowsep, table_content)

            for line_txt in lines_txt:
                self.__parse_and_append_one_record(line_txt)

            df = pd.DataFrame(self.__extracted_records); self.__extracted_records = {} # immediatly empty this variable
            df['src_file'] = self.pdf_file_path + f'; Page: %s'%(page + 1)
            try: 
                final_dataframe = pd.concat([final_dataframe, df], ignore_index=True)
            except: 
                print("Ingestion Failed at {page} of file {self.pdf_file_path}.\n")
                print("The last data frame looks like:")
                print(df.head(3))
                raise Exception("Error concat data frame")
        return final_dataframe

    def get_concatenated_dataframe(self):
        if not self.dfs:
            raise ValueError("No dataframes have been processed. Call parse_dataframe() first.")
        else:
            concatenated_df = pd.concat(self.extracted_dfs, ignore_index=True)
            # self.reset_tmp_vars()
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
        