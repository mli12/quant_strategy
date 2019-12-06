#  -*- coding: utf-8 -*-

from pymongo import UpdateOne
from database import DB_CONN
import tushare as ts
from datetime import datetime

"""
obtain daily OCHL info from tushare, save to local database (MongoDB)
"""


class DailyCrawler:
    def __init__(self):
        """
        initialization
        """
        
        # create daily dataset
        self.daily = DB_CONN['daily']
        
        # create daily_hfq dataset
        self.daily_hfq = DB_CONN['daily_hfq']

    def crawl_index(self, begin_date=None, end_date=None):
        """
        get daily index info: 
        1. keep as record
        2. use as reference for backtesting
        """

        # set which index to crawl
        index_codes = ['000001', '000300', '399001', '399005', '399006']

        # current date
        now = datetime.now().strftime('%Y-%m-%d')
        
        # set begin date 
        if begin_date is None:
            begin_date = now

        # set end date
        if end_date is None:
            end_date = now

        # obtain index info
        for code in index_codes:
            df_daily = ts.get_k_data(code, index=True, start=begin_date, end=end_date)
            self.save_data(code, df_daily, self.daily, {'index': True})

    def save_data(self, code, df_daily, collection, extra_fields=None):
        """
        save data to local database

        :param code: stock symbol
        :param df_daily: DataFrame containing daily prices
        :param collection: dataset to save
        :param extra_fields: extra info to save besids prices
        """

        # request update
        update_requests = []

        # Use the market data in the DataFrame to generate a request to update the data
        for df_index in df_daily.index:
            # converte rows in DataFrame to dict
            doc = dict(df_daily.loc[df_index])
            # set stock symbol
            doc['code'] = code

            # set dict
            if extra_fields is not None:
                doc.update(extra_fields)

            # Generate a database update request
            # add key to code、date、index to avoid decreasing writing speed as size of data increases
            
            db.daily.createIndex({'code':1,'date':1,'index':1})
            update_requests.append(
                UpdateOne(
                    {'code': doc['code'], 'date': doc['date'], 'index': doc['index']},
                    {'$set': doc},
                    upsert=True)
            )

        # If the written request list is not empty, it is saved in the database
        if len(update_requests) > 0:
            # Batch write to the database, batch write can reduce network IO and increase speed
            update_result = collection.bulk_write(update_requests, ordered=False)
            print('save daily prices，symbol： %s, insert：%4d items, update：%4d items' %
                  (code, update_result.upserted_count, update_result.modified_count),
                  flush=True)

    def crawl(self, begin_date=None, end_date=None):
        """
        get stock prices, actual and adjusted

        :param begin_date: beginning date
        :param end_date: ending date
        """

        # get basic information of all stocks through tushare's basic information API
        stock_df = ts.get_stock_basics()
        # Turn an indexed list of basic information into a list of stock symbols
        codes = list(stock_df.index)

        # current date
        now = datetime.now().strftime('%Y-%m-%d')

        # set beginning date
        if begin_date is None:
            begin_date = now

        # set ending date
        if end_date is None:
            end_date = now

        for code in codes:
            # craw actual prices
            df_daily = ts.get_k_data(code, autype=None, start=begin_date, end=end_date)
            self.save_data(code, df_daily, self.daily, {'index': False})

            # craw adjusted prices
            df_daily_hfq = ts.get_k_data(code, autype='hfq', start=begin_date, end=end_date)
            self.save_data(code, df_daily_hfq, self.daily_hfq, {'index': False})


if __name__ == '__main__':
    dc = DailyCrawler()
    # get index daily quotes for a specified date range
    dc.crawl_index('2015-01-01', '2015-12-31')
    # get stock daily quotes for a specified date range
    dc.crawl('2015-01-01', '2015-12-31')
