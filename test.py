import sqlite3
conn = sqlite3.Connection("stock.db")
c = conn.cursor()
tables = [
#    "stock_price",
#    "finance_data",
#    "brand_data",
#    "brand_refresh",
    "stock_condition"
]

for tb in tables:

    sql="select * from %s" % tb
    for row in c.execute(sql):
        print row

conn.close()

