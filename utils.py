from pyspark import SparkContext
#sc = SparkContext()

from pyspark.sql import SQLContext 
#sqlc=SQLContext(sc)
from pyspark.sql.functions import to_date
import unicodedata
def uni_to_string(name):
    return unicodedata.normalize('NFKD', name).encode('ascii','ignore')

from imp import reload

def check_stock_change(sc,sqlc,name, date_list):
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
    
    name_dict = {'tech mahindra':'TECHM_data.csv','unitech':'UNITECH_data.csv','ntpc':'NTPC_data.csv','siemens':'SIEMENS_data.csv','icici bank':'ICICIBANK_data.csv','trident':'TRIDENT_data.csv','tata motors':'TATAMOTORS_data.csv','tata steel':'TATASTEEL_data.csv','symphony':'SYMPHONY_data.csv','house of':'HOPFL_data.csv','maruti suzuki':'MARUTI_data.csv','hdfc bank':'HDFCBANK_data.csv','nirma':'NIRMA_data.csv','jet airways':'JETAIRWAYS_data.csv','infosys':'INFY_data.csv','biocon':'BIOCON_data.csv','hero honda':'HEROHONDA_data.csv','niit':'NIITLTD_data.csv','spanco':'SPANCO_data.csv','reliance power':'RPOWER_data.csv','the indian':'INDHOTEL_data.csv'  ,'bharti airtel':'BHARTIARTL_data.csv','the karnataka':'KTKBANK_data.csv','wipro':'WIPRO_data.csv','ceat':'CEATLTD_data.csv', 'oil &': "OIL_data.csv"}
    filename  = "nse/historical_data/"+name_dict[name]
    c_rdd     = sc.textFile(filename).map(lambda line: line.split(",")) # s_rdd = compnay_Rdd
    header    = c_rdd.first() #[u'Date', u'open', u'high', u'low', u'close', u'adj_close', u'volume']
    c_rdd  = c_rdd.filter(lambda line: line != header)
    try:
        c_df   = sqlc.createDataFrame(c_rdd, header)
    except ValueError:
        print("rdd was empty")
        return name,[0]
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
        change = 0
        t_news_date = cr1.filter(lambda x:(x[0]==news_date)).collect()
        
        if(len(t_news_date)>0 and (t_news_date[0][1]!="null") ):
            #print(t_news_date)
            stock_news_date = float(t_news_date[0][1])                
        else:
            print("ignore this headline as no stock found for this date")
            change= 0 # ignore this headline, it was a sunday maybe
            return_list.append(change)
            continue
        day = news_date.day
        if(day==1):
            prev_day = 30
        else:
            prev_day = day-1
        try:
            prev_date = date(news_date.year, news_date.month, prev_day)
        except:
            print("some error with prev day:",prev_day)
            change=0
            return_list.append(change)
            continue
        t_prev_date = cr1.filter(lambda x:(x[0]==prev_date)).collect()
        
        if(len(t_prev_date)>0):
            stock_prev_date = float(t_prev_date[0][1])
        else:
            # it was a monday maybe, do back
            if(news_date.day<4):
                day=news_date.day+30
            else:
                day = news_date.day
            prev_date = date(news_date.year, news_date.month, day-3)
            t_prev_date = cr1.filter(lambda x:(x[0]==prev_date)).collect()
            
            try:
                stock_prev_date = float(t_prev_date[0][1])
            except:
                print("value present in prev_date but maybe null")
                change=0
                return_list.append(change)
                continue
        perc_change = (stock_news_date-stock_prev_date)*100/stock_prev_date
        if(perc_change>=5):
            print("change observed")
            change=1
        else:
            change=0

        return_list.append(change)
    return name,return_list
