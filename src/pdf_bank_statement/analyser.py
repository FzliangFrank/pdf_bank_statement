from .__import import *


def analyse(dataframe):
    analyser_obj = BankTransactionAnalyser(dataframe)
    return analyser_obj

class BankTransactionAnalyser():
    darkmode = False
    # schema = {
    #     'Date': np.dtype('<M8[ns]'),
    #     'Description': np.dtype('O'),
    #     'Type': np.dtype('O'),
    #     'Money In (£)': np.dtype('float64'),
    #     'Money Out (£)': np.dtype('float64'),
    #     'Balance (£)': np.dtype('float64')
    #     }
    
    category_maper = {
        'Food & Grocery': 'MARKS&SPENCER PLC|TESCO|SAINSBURYS|CO-OP GROUP',
        'Transport': 'STGCOACH|TRAINLINE',
        'Cafe':'COSTA COFFEE|PRET A MANGER',
        'Rent & Essential': 'STEPHEN GIBSON|UKVISA|IMM HEALTH',
        'Subscri & Apple': 'A MEDIUM CORPORATI|EXETER CITY COUNCI|PAYPAL *NETFLIX|APPLE|AO-OPTICALSERVICES|Amazon Prime*HT247|PAYPAL \*LEBARA 150',
        'Cloth & Shopping': 'JOHN LEWIS|TED BAKER',
        }
    
    # Seaborn graphic config
    sns.set_style("whitegrid")
    plot_params = {'color': '0.75',
    'style': '.-',
    'markeredgecolor': '0.25',
    'markerfacecolor': '0.25',
    'legend': False}

    plt.style.use('seaborn-whitegrid')
    plt.rc(
        "figure",
        autolayout=True,
        figsize=(11, 4),
        titlesize=18,
        titleweight='bold',
    )
    plt.rc(
        "axes",
        labelweight="bold",
        labelsize="large",
        titleweight="bold",
        titlesize=16,
        titlepad=10,
    )
    # %config InlineBackend.figure_format = 'retina'

    # Plotly Graph Settings
    
    def __init__(self, data: pd.DataFrame) -> None:
        self.data = data
        self\
            .__categorise()
        #BankTransactionAnalyser.darkmode = darkmode if darkmode is not None else BankTransactionAnalyser.darkmode# initialise dark mode as true
    def __call__(self):
        return self.__categorise()
    
    def __px_bgcolor(self):
        return '#17202A' if BankTransactionAnalyser.darkmode else '#F0F3F4'
        
    def __px_txcolor(self):
        return '#AEB6BF' if BankTransactionAnalyser.darkmode else '#1C2833'
    
    @staticmethod
    def __px_update_fig(fig):
        fig.update_xaxes(
            mirror=True,
            ticks='outside',
            showline=True,
            linecolor='black',
            gridcolor='lightgrey'
        )
        fig.update_yaxes(
            mirror=True,
            ticks='outside',
            showline=True,
            linecolor='black',
            gridcolor='lightgrey'
        )
        return fig
    
    def update_data(self, data=None):
        self.data = self.data if data is None else data
        self.__categorise()
        return self
                
    def __categorise(self):
        description = self.data['Description']

        # loop through predefined category, term mappping
        for value, rex in self.category_maper.items():
            description.replace(rex, value, regex=True,inplace=True)

        self.data.insert(6, "Category", description)

        assert [*self.data.columns] ==  ['Date', 'Description', 'Type', 'Money In (£)', 'Money Out (£)', 'Balance (£)', 'Category', 'src_file']
        return self.data
    
    def add_period(self):
        return self.data.assing(
            Month = lambda df: df.Date.dt.to_period('M'),
            Week = lambda df: df.Date.dt.to_period('W')
        )
    def drop_columns(self):
        self.drop(
            columns=['Category','Month','Week']
        )
        print('columns has been droped, use __categorise to add them back')
        return self
    
    def by_spending_period(self, p='Week'):
        '''Aggregation'''
        spending_periodic_breakdown = (
            self
            .add_period()
            .groupby(['Category'] + [p])
            .agg({
                'Money Out (£)':sum,
                'Description':lambda x: x.value_counts().to_json()
                })
            # .drop('Rent & Essential', axis = 0)
        )
        return spending_periodic_breakdown
    def plot_spending_period(self, p='Week'):
        
        visBase = self.by_spending_period(p)
        # Register for ploting to use 
        ax = plt.gca() # Get Current Plotting
        pd.plotting.register_matplotlib_converters()
        ax.xaxis.freq = visBase.index.levels[1].freq
        plt.xticks(rotation=45)

        # Two Layer Plot
        sns.lineplot(
            data = visBase.reset_index(),
            x = p,
            y = 'Money Out (£)',
            hue = 'Category',
            palette = 'Set2',
            legend = False # Remove the first plot so ledgend don't overlay
        )
        sns.pointplot(
            data = visBase.reset_index(),
            x = p,
            y = 'Money Out (£)',
            hue = 'Category',
            palette = 'Set2'
        )
        sns.move_legend(
            plt.gca(), loc='center right', frameon=False, bbox_to_anchor=(1.2, .5)
        )
    def plotly_spending_period(
            self,p='Week',type='line'
            ):
        visBase=self.by_spending_period(p)
        visBase=visBase.reset_index()
        visBase[p]=visBase[p].dt.to_timestamp()
        pal = px.colors.qualitative.Vivid

        bgcolor=self.__px_bgcolor()
        txtcolor=self.__px_txcolor()

        if p=='Month':
            income_level=2199.5
        elif p=='Week':
            income_level=2199.5/4


        if(type=='line'):
            fig = px.line(
                visBase, 
                x=p, 
                y='Money Out (£)',
                color='Category',
                markers=True,
                color_discrete_sequence=pal
            )
        elif(type=='bar'):
            fig = px.bar(
                visBase, 
                x=p, 
                y='Money Out (£)',
                color='Category',
                color_discrete_sequence=pal
            )
        fig.add_hline(y=income_level)
        fig.update_layout(
            font=dict(
                color=txtcolor
            ), 
            plot_bgcolor=bgcolor,
            paper_bgcolor=bgcolor,
            legend_title_text='',
            legend=dict(
                orientation="h",
                entrywidth=100,
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ))
        fig.update_xaxes(
            mirror=True,
            ticks='outside',
            showline=True,
            linecolor='black',
            # gridcolor='lightgrey'
        )
        fig.update_yaxes(
            mirror=True,
            ticks='outside',
            showline=True,
            linecolor='black',
            gridcolor='lightgrey'
        )
        # fig.show(config=config) # don't ever do this! it will open up a new window
        return fig
    def plot_balance(self):
        p = sns.scatterplot(
            data = self.data,
            x = 'Date',
            y = 'Balance (£)'
        )
        p.set_title('Blance Level')
    def plotly_balance(self):
        safty=3700
        safty_color="#e63e06"

        fig = go.Figure(go.Scatter(
            mode='markers',
            x=self['Date'],
            y=self['Balance (£)'],
            marker=dict(
                color='#000080',
                line=dict(#line configure border color
                    color='#FFFFFF',
                    width=1
                )
            )
        ))
        fig.add_hline(
            y=safty,
            line=dict(
                color=safty_color,
                width=2,
                dash="dot"
            ),
            label=dict(# Lable property add annotation directly on the marker.
                text='SAFTY FIRST:)', 
                font=dict(color=safty_color), 
                textposition='end',yanchor='top'
                )
        )
        fig.update_layout(
            font=dict(
                color=self.__px_txcolor(),
            ),
            plot_bgcolor=self.__px_bgcolor(),
            paper_bgcolor=self.__px_bgcolor(),
        )
        self.__px_update_fig(fig)
        return fig
        


            
        
    