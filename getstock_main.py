#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import datetime
import pandas as pd
import ConfigParser
import jsm
from getstock import StockDB

reload(sys)
sys.setdefaultencoding('utf-8')


def main():
    
    conf = ConfigParser.SafeConfigParser()
    conf.read("stock.ini")
    db = StockDB()
    db.refreshDBCon() 
    """DBテーブルの生成"""
    db.create_db()
    db.Commit()

    """jsmの初期化"""
    q = jsm.Quotes()


    """Brandの取得"""
    b = jsm.Brand()
    IDS = b.IDS
        
    for industry_code in IDS.keys():
        """Brandの更新日付を取得する"""
        is_brand_get = 0
        update_span = int(conf.get("CONFIG","brand_refresh"))
        brand_updated = db.GetDateBrandRefreshed(industry_code)

        if brand_updated is None:
            is_brand_get = 1
        else:
            today = datetime.date.today()

            if brand_updated <= today - datetime.timedelta(days=update_span):
                is_brand_get = 1
        
        if is_brand_get == 1:
            print "Retrieving Industory Code: " + industry_code
            industry_name = IDS[industry_code]
            brand_data = q.get_brand(industry_code)

            list_of_dict_brand = [{"ccode":brand.ccode,
                                   "industry_code":industry_code,
                                   "industry_name":industry_name,
                                   "market":brand.market,
                                   "name":brand.name,
                                   "info":brand.info
                                  } for brand in brand_data]

            db.InsertBrandData(list_of_dict_brand)
            db.UpdateBrandRefreshed(industry_code)
            db.Commit()
        else:
            #print "Skip Industory Code: " +industry_code
            pass

    """CCODEの取得"""
    ccodes = db.GetCCode()
    if ccodes is None:
        return 1

    """Financial Dataの取得"""
    list_of_dict_finance = []
    cnt = 0
    for ccode in ccodes:
	is_finance_get = 0
        update_span = int(conf.get("CONFIG","finance_refresh"))
        finance_updated = db.GetDataFinanceUpdated(ccode)
        if finance_updated is None:
            is_finance_get = 1
        else:
            today = datetime.date.today()

            if finance_updated <= today - datetime.timedelta(days=update_span):
                is_finance_get = 1

        if is_finance_get == 1:
            print "Retrieving Financial Data for: " + str(ccode)
            try:
                finance_data = q.get_finance(ccode)
                list_of_dict_finance.append({"ccode":ccode,
                                        "market_cap":finance_data.market_cap,
                                        "shares_issued":finance_data.shares_issued,
                                        "dividend_yield":finance_data.dividend_yield,
                                        "dividend_one":finance_data.dividend_one,
                                        "per":finance_data.per,
                                        "pbr":finance_data.pbr,
                                        "eps":finance_data.eps,
                                        "bps":finance_data.bps,
                                        "price_min":finance_data.price_min,
                                        "round_lot":finance_data.round_lot})
                cnt += 1
	        db.InsertFinancialData(list_of_dict_finance)
    	        db.Commit()
            except Exception as e:
	        print e.message
                print "Error in Financial Data " , ccode
	else:
	    #print "Skip Financial Data for:" + str(ccode)
	    pass
#    db.InsertFinancialData(list_of_dict_finance)
#    db.Commit()

    """Quoteの取得"""
    cnt = 0
    commit_limit = 1 
    list_of_dict_stock = []
    default_start_date = datetime.date(2000,1,1)
    default_end_date = datetime.date.today()
    for ccode in ccodes:
        #if cnt > commit_limit:
        #    break
        """DBよりccode別の開始日付を取得する"""
        start_date = db.GetStartDate(ccode)
        if start_date is None:
            """存在しなければ2000/1/1から取得する"""
            start_date = default_start_date

        """デフォルトの終了日付は本日"""
        end_date = default_end_date

        try:
            if start_date >= end_date:
                """開始時点が今日もしくはそれ以降の場合は取得できない。"""
                quote_data = None
            elif not db.isStockUpdated(ccode, start_date, end_date):
		print "Retrieve Stock Price for: %s" %ccode
                quote_data = q.get_historical_prices(ccode,jsm.DAILY,start_date=start_date,end_date=end_date)
		print "Done."
	    else:
		quote_date = None

            cnt += 1

            if quote_data is None:
                continue

            [list_of_dict_stock.append({"ccode":ccode,
                 "date":quote.date,
                 "open":quote.open,
                 "high":quote.high,
                 "low":quote.low,
                 "close":quote.close,
                 "volume":quote.volume}) for quote in quote_data]

            db.UpdateStockCondition(ccode,1,default_start_date.strftime("%Y-%m-%d"),end_date.strftime("%Y-%m-%d"))

        except :

            db.UpdateStockCondition(ccode,0,default_start_date.strftime("%Y-%m-%d"),end_date.strftime("%Y-%m-%d"))

        if cnt >= commit_limit:
            db.InsertStockData(list_of_dict_stock)
            #db.Commit()
            cnt = 0
            list_of_dict_stock = []

    if len(list_of_dict_stock) > 0:
        db.InsertStockData(list_of_dict_stock)
        #db.Commit()

    db.Close()

    return 0


if __name__ == '__main__':

    sys.exit(main())
