#!/usr/bin/python
import sqlite3
import jsm
import datetime
import mysql.connector as mysql


class StockDB:
    def __init__(self):
        self.conn = sqlite3.connect('stock.db')
        self.conn.text_factory = str
        self.c = self.conn.cursor()
    
    def refreshDBCon(self):
	self.conn.close()
	self.conn = sqlite3.connect('stock.db')
	self.c = self.conn.cursor()

    def create_db(self):
        #Table for Stock price
        sql ='''CREATE TABLE IF NOT EXISTS stock_price (
                                 ccode int(11),
                                 date datetime,
                                 open double,
                                 high double,
                                 low double,
                                 close double,
                                 volume double,
                                 updatetime timestamp,
                                 PRIMARY KEY(ccode,date)
                                 ) '''
        self.c.execute(sql)

        #Table for Finance
        sql = '''CREATE TABLE IF NOT EXISTS finance_data(
                                 ccode int(11),
                                 market_cap double,
                                 shares_issued double,
                                 dividend_yield double,
                                 dividend_one double,
                                 per double,
                                 pbr double,
                                 eps double,
                                 bps double,
                                 price_min double,
                                 round_lot double,
                                 updatetime timestamp,
                                 PRIMARY KEY(ccode)
                                 )'''
        self.c.execute(sql)

        #Table for Brand
        sql = '''CREATE TABLE IF NOT EXISTS brand_data(
                                 ccode int(11),
                                 industry_code int(11),
                                 industry_name varchar(32),
                                 market varchar(32),
                                 name varchar(128),
                                 info varchar(256),
                                 updatetime timestamp,
                                 PRIMARY KEY(ccode)
                                 ) '''
        self.c.execute(sql)

        #Table for Brand update info
        sql = '''CREATE TABLE IF NOT EXISTS brand_refresh(
                                 industory_code int(11),
                                 updatetime timestamp
                                 )'''
        self.c.execute(sql)

        #Table for stock condition update info
        sql = '''CREATE TABLE IF NOT EXISTS stock_condition(
                                 ccode int(11),
                                 completed int(11),
                                 start_date datetime,
                                 end_date datetime,
                                 updatetime timestamp,
                                 PRIMARY KEY(ccode)
                                 )'''

        self.c.execute(sql)

    def GetDataFinanceUpdated(self, ccode):
        try:
            sql = "SELECT ccode, DATE(updatetime) from finance_data where ccode = %s" % ccode
            self.c.execute(sql)

        except mysql.Error:
            print "Error Occured in Select Finance Update"

        rows = self.c.fetchall()
        if rows is None:
            return None
        for r in rows:
            #print rows
            ret = r[1]
            #print ret
            ret = datetime.datetime.strptime(ret, "%Y-%m-%d")
            return ret.date()

    def GetDateBrandRefreshed(self, industory_code):
        """
        Get refresh date of Brand
        :return: refresh data
        """
        try:
            sql = "SELECT industory_code,DATE(updatetime) from brand_refresh where industory_code = %s" % industory_code
            self.c.execute(sql)

        except mysql.Error:
            print "Error Occured in Select Brand Refreshed "

        rows = self.c.fetchall()
        if rows is None:
            return None
        for r in rows:
            #print rows
            ret = r[1]
            #print ret
            ret = datetime.datetime.strptime(ret, "%Y-%m-%d")
            return ret.date()

    def isStockUpdated(self, ccode, start_date, end_date):
	try:
	    sql = "SELECT start_date, end_date from stock_condition where ccode = %s" % ccode
	    for row in self.c.execute(sql):
	        if rows is None:
	            return False
	        elif rows[0] > start_date or rows[1] < end_data:
	            return True
	        else:
	            return False
	    
	except Exception as e:
	    print e.message

    def GetCCode(self):
        """
        Get list of CCODE
        :return:list
        """
        try:
            sql = "SELECT ccode from brand_data order by ccode;"
            self.c.execute(sql)
        except mysql.Error:
            print "Error Occured in Select CCode"
        rows = self.c.fetchall()
        if rows is None:
            return None
        ret = []
        for r in rows:
            ret.append(r[0])
        return ret

    def GetStartDate(self,ccode):
        """
        Get start date for each CCODE
        :param ccode: ccode
        :return: date
        """
        try:
            sql = "select ccode,completed,DATE(start_date),DATE(end_date) from stock_condition where ccode = " + str(ccode)
            self.c.execute(sql)
        except mysql.Error:
            print "Erro Occured in Select Start Date"
        rows = self.c.fetchall()
        if rows is None:
            return None
        for r in rows:
            if r[1] == 0:
                return r[2] #start date
            return r[3]


    def UpdateStockCondition(self,ccode,completed,start_date,end_date):
        """
        Update condition for retrieving Stock Price
        :param ccode:
        :param completed:
        :param start_date:
        :param end_date:
        :return:
        """
        try:
            sql = "REPLACE into stock_condition(ccode,completed,start_date,end_date) values(?,?,?,?);"
            self.c.execute(sql,(ccode,completed,start_date,end_date))
        except mysql.Error:
            print "Error update stock condition in ",ccode


    def UpdateBrandRefreshed(self,industory_code=0):
        """
        Update BrandRefresh Table
        :param cnt:
        :return:
        """
        try:
            sql = "REPLACE INTO brand_refresh(industory_code, updatetime) VALUES(?,?)"
            self.c.execute(sql,(industory_code,datetime.date.today()))
            print "Update Brand: " + industory_code
        except mysql.Error:
            print "Error Occured in update brand refreshed"

    def InsertStockData(self,list_of_dict=[]):
        """
        Insert price to StockPrice table
        :param list_of_dict: list of dict
        :return:
        """
        if len(list_of_dict) == 0:
            return None
        try:
            sql = "REPLACE INTO stock_price" + "(ccode, date, open, high,low,close,volume,updatetime) VALUES"
            ss = "(?,?,?,?,?,?,?,?)"
            sql += ss 
            params_list = []
            for row in list_of_dict:
                tmp = (row["ccode"],row["date"],row["open"],row["high"],row["low"],row["close"],row["volume"],datetime.datetime.now())
		self.c.execute(sql,tmp)

        except Exception as e:
	    print e.message
	
	self.Commit()

    def InsertBrandData(self,list_of_dict=[]):
        """
        Insert Brand data
        :param list_of_dict:
        :return:
        """
        if len(list_of_dict) == 0:
            return None
        try:
            sql = "REPLACE INTO brand_data" + "(ccode, industry_code, industry_name, market,name,info) VALUES"
            ss  = " (?,?,?,?,?,?)"
            sql += ss
            
            params_list = []
            for row in list_of_dict:
                tmp = (row["ccode"],row["industry_code"],row["industry_name"],row["market"],row["name"],row["info"])
                #[params_list.append(x) for x in tmp]
                params_list.append(tmp)

            self.c.executemany(sql, params_list)

        except mysql.Error:
            print "Error Occurred in insert "

    def InsertFinancialData(self,list_of_dict=[]):
        """
        Insert financial data
        :param list_of_dict:
        :return:
        """
        try:
            sql = "REPLACE INTO finance_data (ccode, market_cap, shares_issued, dividend_yield,dividend_one,per,pbr,eps,bps,price_min,round_lot,updatetime) VALUES"
            ss = "(?,?,?,?,?,?,?,?,?,?,?,?)"
            sql += ss
            
            params_list = []
            for row in list_of_dict:
                tmp = (row["ccode"],row["market_cap"],row["shares_issued"],row["dividend_yield"],row["dividend_one"],row["per"],row["pbr"],row["eps"],row["bps"],row["price_min"],row["round_lot"],datetime.date.today())
                params_list.append(tmp)
            self.c.execute(sql,tmp)
            #self.c.executemany(sql,params_list)
        except mysql.Error:
            print "Error Occurred in insert "


    def Commit(self):
        self.conn.commit()

    def Rollback(self):
        self.conn.rollback()


    def Close(self):
	self.conn.close()
