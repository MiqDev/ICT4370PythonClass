"""
    Author: Miquel Rael
    Date: 8/6/2023
    Purpose: Classes for creating a database, creating tables, querying database and creating lists
"""

import sqlite3
import csv
import pandas as pd

class SetUpDb:

    def __init__(self,database_name):
        """ initialize database """
        self.databaseName = database_name

    def create_tables(self):
        """ create tables in database """
        db_conn = sqlite3.connect(self.databaseName)
        db_cursor = db_conn.cursor()

        create_table1 = """
            create table if not exists close_hx(
                stock_symbol text not null ,
                close_date date not null ,
                close_price float not null
            )
        """

        db_cursor.execute(create_table1)

        create_table2 = """
            create table if not exists stocks(
                symbol text primary key,
                no_shares integer not null
                )
        """
        db_cursor.execute(create_table2)

        db_conn.commit()
        db_conn.close()

    def load_stock_data(self,csv_data):
        """ load stock data into database """

        db_conn = sqlite3.connect(self.databaseName)
        db_cursor = db_conn.cursor()

        sql = """
             INSERT INTO stocks (
                 symbol,
                 no_shares
                 )
             VALUES(?,?)
             """

        for row in csv_data:
            try:
                values = [
                    row['SYMBOL'], row['NO_SHARES']
                ]
            except KeyError:
                continue
            else:
                try:
                    db_cursor.execute(sql, values)
                except sqlite3.IntegrityError:
                    print(row['SYMBOL'] + " already loaded in database.")

        sql_update = """
            update stocks 
            set symbol = 'GOOG'
            where symbol = 'GOOGL'
        """
        db_cursor.execute(sql_update)

        db_conn.commit()
        db_conn.close()

    def load_close_hx_many(self,list_tuples):
        """ load historical close price and close dates into database """
        db_conn = sqlite3.connect(self.databaseName)
        db_cursor = db_conn.cursor()

        sql = """
             INSERT INTO close_hx (
                 stock_symbol,
                 close_date,
                 close_price
                 )
             VALUES(?,?,?)
             """
        try:
            db_cursor.executemany(sql,list_tuples)
        except sqlite3.OperationalError as e:
            if e.args[0].startswith('no such table'):
                print("Table named 'close_hx' not found")
            elif e.args[0].find('has no column named'):
                print("Column names from query do not match what exists in 'close_hx'.")

        db_conn.commit()
        db_conn.close()

class Stocks:

    def __init__(self,stock_symbol,close_price,close_date):
        """ initialize stock symbol, close price, and close date"""
        self.stockSymbol = stock_symbol
        self.closePrice = close_price
        self.closeDate = close_date

class GetStockData:

    def __init__(self,database_name):
        """ initialize database and set up three lists """
        self.databaseName = database_name
        self.stockList = []
        self.dateList = []
        self.stockValuelist = []

    def get_stock_symbol_list(self):
        """ query database and get list of stock symbols """
        db_conn = sqlite3.connect(self.databaseName)
        db_cursor = db_conn.cursor()

        sql = """
                select 
                    symbol
                from stocks
                """

        sqlExecute = db_cursor.execute(sql)
        sqlResults = sqlExecute.fetchall()

        db_conn.commit()
        db_conn.close()

        stock_list = []
        for result in sqlResults:
            stock_list.append(result[0])

        return stock_list

    def load_lists(self,find_symbol):
        """ query database and create lists for a stock symbol """
        db_conn = sqlite3.connect(self.databaseName)
        db_cursor = db_conn.cursor()

        sql = """
            select 
                close_date
                , round( close_price * no_shares , 2 ) as stockValue
            from close_hx h
                inner join stocks s
                on h.stock_symbol = s.symbol
            where h.stock_symbol = """

        sql += "'" + find_symbol + "'"

        sql += "\norder by close_date"

        sqlExecute = db_cursor.execute(sql)
        sqlResults = sqlExecute.fetchall()

        db_conn.commit()
        db_conn.close()

        for result in sqlResults:
            self.dateList.append(result[0])
            self.stockValuelist.append(result[1])

        return self.dateList, self.stockValuelist

    def get_data_into_df(self,stock_list):
        """ query database and create lists for a stock symbol """

        db_conn = sqlite3.connect(self.databaseName)
        db_cursor = db_conn.cursor()

        sql_qry_final = """
            select 
                s.symbol ,
                a.close_date ,
                a.close_price * s.no_shares as stock_value
            from stocks s
            inner join close_hx a 
            on s.symbol = a.stock_symbol
            where 1=1
            order by 2,1
        """

        # print(sql_qry_final)
        sqlExecute = db_cursor.execute(sql_qry_final)
        cols = [column[0] for column in sqlExecute.description]
        sqlResults = sqlExecute.fetchall()
        qry_df = pd.DataFrame.from_records(data=sqlResults,columns=cols)
        db_conn.commit()
        db_conn.close()

        return qry_df
