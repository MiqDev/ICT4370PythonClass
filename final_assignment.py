"""
    Author: Miquel Rael
    Date: 8/6/2023
    Purpose:
        get data from json file, load into a database, and pull data from database and plot
        in a time series line graph showing the stock value for stocks over time.
"""

import json
import csv
import portfolios
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib
import pandas as pd

# define file names
database_name = 'stocks.db'
file_name = 'AllStocks.json'
stock_filename = 'Lesson6_Data_Stocks.csv'

# load json
try:
    with open(file_name) as jsonfile:
        data_file = json.load(jsonfile)
except FileNotFoundError:
    print(f"{file_name} not found.  Quitting program.")
    quit()

# load csv data
try:
    csv_data = csv.DictReader(open(stock_filename))
except FileNotFoundError:
    print(f"{stock_filename} not found.  Quitting program.")
    quit()

# set up database and create tables
p = portfolios.SetUpDb(database_name=database_name)
p.create_tables()

# load csv data into database
p.load_stock_data(csv_data=csv_data)

# create a data list from json data
data_list = []
for stock in data_file:
    stock_item = portfolios.Stocks(
        stock_symbol=stock['Symbol'],
        close_price=stock['Close'],
        close_date=stock['Date']
    )
    data_temp = []
    data_tuple = ()
    data_temp.append(stock_item.stockSymbol)
    data_temp.append(datetime.strptime(stock_item.closeDate,'%d-%b-%y').date())
    data_temp.append(float(stock_item.closePrice))
    data_tuple = tuple(data_temp)
    data_list.append(data_tuple)

# insert json data into database table
p.load_close_hx_many(data_list)

# initialize database
d = portfolios.GetStockData(database_name=database_name)

# create a stock list and dataframe of dataset to plot
stock_list = d.get_stock_symbol_list()
qry_df = d.get_data_into_df(stock_list)

# get list of dates, stocks, and close amounts and plot on a line chart
df_close_alldates = qry_df[['symbol','close_date','stock_value']]
df_close_alldates.set_index('close_date',inplace=True)
df_close_alldates.groupby('symbol')['stock_value'].plot(legend=True,xlabel='Close Date',ylabel='Value',title='History of Stock Valuation')

sMessage = "Do you want to save the plot to file or print to screen?\n"
sMessage += "Enter 1 for save to file or 2 for print to screen: "
if input(sMessage) == "1":
    plt.savefig('HistoryOfStockValuation.jpg')
else:
    plt.show()
