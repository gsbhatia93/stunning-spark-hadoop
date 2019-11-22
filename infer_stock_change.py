#infer_stock_change.py

import utils
# open in a new terminal 
import pickle
nr10 = open("nr10",'r')
news_data = pickle.load(nr10)

reload(utils)
change_data = []
for data in news_data:
	if(len(data[1])<15):
		continue
	print(data[0])	
	name,change_list = utils.check_stock_change(sc,sqlc,data[0],data[1])
	change_data.append((data[0],change_list))

