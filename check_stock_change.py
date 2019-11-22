from pyspark.sql import SQLContext 
from pyspark.sql.functions import to_date
import unicodedata
def uni_to_string(name):
	return unicodedata.normalize('NFKD', name).encode('ascii','ignore')

sqlc=SQLContext(sc)

def check_stock_change(name, date_list):
	'''
		name: company name
		date_list = ResultIterable of dates wben the compnay was mentioned
					in the headline
		Output: name, 1/0 for every date
					  1 = positive change happended 	
					  0 = no change happended 	
					 -1 = neg change happened
					100 = error code, date not found in the file, must be weekend
			  : correspnds to flatMap as returning multiple values

		~ Open csv file corresposning to the company name
		~ check stock value t-1,t,t+1 	
	'''
	from datetime import date
	name = "wipro"	
	name_dict = {'wipro':'WIPRO_data.csv'}
	filename  = "nse/historical_data/"+name_dict["wipro"]
	c_rdd  = sc.textFile(filename).map(lambda line: line.split(",")) # s_rdd = compnay_Rdd
	header = c_rdd.first() #[u'Date', u'open', u'high', u'low', u'close', u'adj_close', u'volume']
	c_rdd  = c_rdd.filter(lambda line: line != header)
	c_df   = sqlc.createDataFrame(c_rdd, header)
	c_df   = c_df.drop(header[1],header[2],header[3],header[5],header[6])
	c_df   = c_df.na.drop(subset=["close"])
	c_df2  = c_df.withColumn("stock_date", to_date(c_df.Date, 'yyyy-MM-dd'))
	cr     = c_df2.rdd

	# change date, close format
	cr1 = cr.map(lambda x:(x[2],uni_to_string(x[1]))  )

	# fetch stock data using dates from csv
	# a (b) c 
	# a = d-1 b=d.  c=d-1
	return_list=[]
	for news_date in date_list:
		# news_date = date(2008,1,1)
		change = 0
		t_news_date = cr1.filter(lambda x:(x[0]==news_date)).collect()

		if(len(t_news_date)>0):
			stock_news_date = float(t_news_date[0][1])
		else:
			change= 0 # ignore this headline, it was a sunday maybe

		prev_date = date(news_date.year, news_date.month, news_date.day-1)
		t_prev_date = cr1.filter(lambda x:(x[0]==prev_date)).collect()

		if(len(t_prev_date)>0):
			stock_prev_date = float(t_prev_date[0][1])
		else:
			# it was a monday maybe, do back
			prev_date = date(news_date.year, news_date.month, news_date.day-3)
			t_prev_date = cr1.filter(lambda x:(x[0]==prev_date)).collect()
			stock_prev_date = float(t_prev_date[0][1])

		perc_change = (stock_news_date-stock_prev_date)*100/stock_prev_date
		if(perc_change>=5):
			change=1
		else:
			change=0		

		return_list.append((name,chnage))	
	return return_list
