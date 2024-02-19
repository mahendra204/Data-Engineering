# import  pandas as pd  
# data=pd.read_csv ('data\Steel_industryinput.csv')
# datacols=data.columns
# data=data[datacols].str.upper()
# data.to_csv('C:\Users\mahen\Desktop\projml\mlops\mahlops\output.csv', index=False)

import pandas as pd

# Read the CSV file
data = pd.read_csv('data\\Steel_industryinput.csv')

# Define the output file path
# output_file = 'C:\\Users\\mahen\\Desktop\\projml\\mlops\\mahlops\\output.csv'
outfile1='C:\\Users\\mahen\\Desktop\\projml\\mlops\\files\\out.csv'

data.columns=data.columns.str.upper()
data['DATE_TIME'] = pd.to_datetime(data['DATE_TIME'], format='%d/%m/%Y %H:%M')

outputdata=data[(data['LOAD_TYPE']=='Light_Load') & (data['DATE_TIME'].dt.month == 6) & (data['DAY_OF_WEEK']=='Saturday')]

# Store the data in the output CSV file
outputdata.to_csv(outfile1, index=False)
