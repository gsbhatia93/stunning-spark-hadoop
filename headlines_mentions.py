
'''

scans the headlines text and gets the count of headlines
that mnetion any one of the companies in the database

https://www.kaggle.com/gsbhatia93/visualisation-and-sentiment-analysis-of-toi/edit

'''

from pyspark.sql import SQLContext 
from pyspark.sql.functions import to_date

import unicodedata
def uni_to_string(name):
	return unicodedata.normalize('NFKD', name).encode('ascii','ignore')

sqlc=SQLContext(sc)

# this one works to read and convert to proper format
# nr = news_rdd
nr = sc.textFile("india-news-headlines.csv").map(lambda line: line.split(","))

header = nr.first()
# get header info and remove header
nr = nr.filter(lambda line: line != header)   #filter out header
nr1 = nr.map(lambda x:(x[0],x[1],x[2].replace('"','')))
# ndf = news_df
ndf = sqlc.createDataFrame(nr1, header)

ndf.show()

# make new DF with date values properly defined
ndf2 = ndf.withColumn("date_publish", to_date(ndf.publish_date, 'yyyyMMdd'))
ndf2.registerTempTable("news_headlines")

nq1 = sqlc.sql("select headline_category, headline_text, date_publish from news_headlines where date_publish > '2008-01-01' and date_publish < '2009-01-01' ")

nq1.count() # 124893

nr2 = nq1.rdd

nr3 = nr2.map(lambda x:x[1].lower())
nr4 = nr3.map(lambda x:uni_to_string(x))
nr5 = nr4.cache()
# ------------------ reading the company name csv ----------------------- #
stock_rdd = sc.textFile("Companies_list.csv").map(lambda line: line.split(","))
header_2 = stock_rdd.first()
stock_rdd = stock_rdd.filter(lambda line: line != header_2)

header_2[0] = "to_be_deleted"
stock_df = sqlc.createDataFrame(stock_rdd, header_2)
stock_df = stock_df.drop(header_2[0])

# sr = stock_rdd

sr = stock_df.rdd
sr1 = sr.map(lambda x: (uni_to_string(x[0]),uni_to_string(x[1])) )
sr2 = sr1.map(lambda x: (x[0],x[1].split("_")))

sr3 = sr2.filter(lambda x: "Limited" in x[1])
sr4 = sr3.map(lambda x:(x[0],x[1][0:len(x[1])-1]))
sr5 = sr4.map(lambda x:" ".join(x[1]).lower())
sr6 = sr5.filter(lambda x:len(x)>3)

company_names = sr6.collect()
company_names.remove("d b reality")
company_names.remove("k s oils")
company_names.remove('w s industries (i)')
company_names.remove('premier')
company_names.remove('gati')

# -------------------------------------------------
def check_name_in_headline(headline):
	ignore_list = ['moil','d b', 'the great', 'c &','atul','arvind','new delhi','tamil nadu','the state']
	for name in company_names:
		name = name.split(" ")
		name = " ".join(name[0:2])
		if(name in ignore_list):
			continue
		if(name in headline):
			print(name, headline)
			return 1
	return 0
# -----------------------
# RQ1 : check for name in headline 
# compare nr4 with compnay names
nr6 = nr5.map(lambda x:(x,check_name_in_headline(x)))
nr7 = nr6.reduceByKey(lambda x,y:x+y)
nr8 = nr7.filter(lambda x:x[1]==1)
nr8.count() # 794

# ----------------
# check which companies they are talking about
mentioned_name_set = set()

def find_name_in_headline(headline):
	ignore_list = ['the indian','moil','d b', 'the great', 'c &','atul','arvind','new delhi','tamil nadu','the state']
	for name in company_names:
		name = name.split(" ")
		name = " ".join(name[0:2])
		if(name in ignore_list):
			continue
		if(name in headline):
			print(name, headline)
			return 1,name
	return 0

nr9 = nr8.map(lambda x:(x[0],find_name_in_headline(x[0])))
# x0 is headline x10 is count x11 compnay name, 
nr10 = nr9.map(lambda x:(x[1][1],x[1][0])) 

nr11 = nr10.reduceByKey(lambda x,y:x+y)
nr12 = nr11.sortBy(lambda x:-x[1])


mentioned_name_set = set(nr11.collect()) 

