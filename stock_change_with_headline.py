'''

Does a newspaper headline affect stock price

For these companies, check if their stock value altered 
newspaper headline t

stock compare t,t-1
stock compare t,t+1

('wipro', 77), ('infosys', 72), ('tata motors', 56), 
('icici bank', 32), ('ntpc', 29), ('jet airways', 21), 
('unitech', 20), ('tech mahindra', 19), ('nirma', 18), 
('tata steel', 18), ('reliance power', 15)

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
ndf = sqlc.createDataFrame(nr1, header) # ndf = news_df

ndf.show()

# make new DF with date values properly defined
ndf2 = ndf.withColumn("date_publish", to_date(ndf.publish_date, 'yyyyMMdd'))
ndf2.registerTempTable("news_headlines")

nq1 = sqlc.sql("select headline_category, headline_text, date_publish from news_headlines where date_publish > '2008-01-01' and date_publish < '2009-01-01' ")

nq1.count() # 124893

nr2 = nq1.rdd # headline , date_published
nr3 = nr2.map(lambda x:(uni_to_string(x[1]).lower(),x[2]))
nr5 = nr3.cache()
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
def find_name_in_headline(headline):
	ignore_list = ['the south','the india','moil','d b', 'the great', 'c &','atul','arvind','new delhi','tamil nadu','the state']
	for name in company_names:
		name = name.split(" ")
		name = " ".join(name[0:2])
		if(name in ignore_list):
			continue
		if(name in headline):
			print(name, headline)
			return 1,name
	return 0, "000-None"


# RQ1 : check for name in headline 
# compare nr5 with company names
# headline, date, (1/0, company_name)
nr6 = nr5.map(lambda x:(x[0],x[1],find_name_in_headline(x[0]))) 
nr7 = nr6.filter(lambda x:x[2][0]==1)
nr8 = nr7.map(lambda x: (x[2][1],x[1])) # name, date,1/0
nr8.cache()
nr9 = nr8.groupByKey()

# (name,[datetime1,datetime2])

nr10 = nr9.map(lambda x: (x[0],list(x[1]))) 
mylist = nr10.collect()

# this is not working so i am trying to pickle and open in another 
# spark context

pickle_name = "nr10"
file_obj = open(pickle_name,'wb')
pickle.dump(file_obj,mylist)
file_obj.close()

# nr11 = nr10.flatMap(lambda x: utils.check_stock_change(sc,sqlc,x[0], x[1]))




# ----------------
# check which companies they are talking about
mentioned_name_set = set()

nr9 = nr8.map(lambda x:(x[0],find_name_in_headline(x[0])))
# x0 is headline x10 is count x11 compnay name, 
nr10 = nr9.map(lambda x:(x[1][1],x[1][0])) 

nr11 = nr10.reduceByKey(lambda x,y:x+y)
nr12 = nr11.sortBy(lambda x:-x[1])

mentioned_name_set = set(nr11.collect()) 




