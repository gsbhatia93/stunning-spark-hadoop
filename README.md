# stunning-spark-hadoop
<h3>Headline influence on Stock Value of a company</h3>

Motivation:
Stock market is volatile and get affected very easily but rumours and news, very quickly. If a company is not doing well the volume of its shares traded can increase significantly. There is also quite evident change in the stock value. Stock market is based on beliefs. Traders bet on a company doing well or not doing well. To use the more financial terms of Bullish or Bearish. If people feel the company is going to do well, they buy its stock. If they don’t, they sell. Whether it actually does well or goes bankrupt is part of the game, no one can really predict future. When a lot of people feel the company will do well, they buy its stock. What happens when a lot of people are buying the stocks of a company? It stock values increases. What happens when a bad news related to the company comes out it the morning right before the start of trading day? A lot of people who own the stock will see it, sending its value down.
In my project I try to map this relations where I what happens to stock market value of company when it was in news. As an added question, I also look at a significant major event alters the way in which newspaper talk about.
  
  
<h5>Data Sets:</h5>
   
* NSE Listed 1000+ Companies Historical Dataset: This dataset contains the stock values   from over 1000 publicaly traded companies in India on the NSE stock market. India has two stock markets, namely BSE and NSE. The dataset I have used relates to the National Stock Exchange (NSE) market. It contains, (1) list of all companies and their trading symbols and (2) a csv file for every company containing the following rows.
* Columns used:
  * Date:recorddate
  * Sub Item 2
  * Open:openingvalueofstockforthedate
  * High:higheststockvaluewentduringtheday
  * Low:loweststockvaluewentduringtheday
  * Close:Closingvalue
  * Volume: Volume of shares trades
  
  
<h5> Analysis and Visualization: </h5>
<h6> Research Question 1: Find out the list and number of times a company features in the headlines. </h6>


- After my first join, where I have names of all companies in the dataset, I scan through each headline, using pyspark, and check whether the headline contains the company name or not. I had to shorten the company name used for comparison, or just take top 2 words as full name might not often be used in the headline.
- Code for this is mentioned in headline_mentioned.py and after joining, I run every line through the find_name_in_headline function. It returns (1,name_of_company) for every company there is match. For my simple case I assume just one company per headline.
- Reducing Noise: had to perform some manual scrapping to remove some company names that gave me false positives.
- Then I count the number of times each company was mentioned using reduceByKey function.

![alt text](https://github.com/your-free/stunning-spark-hadoop/blob/master/stock_changes.png)

<h6>Research Question 2: Find out if the day a company was featured in a headline, its stock value changed in comparison to previous day. </h6>

- Code file: stock_change_with_headline.py and check_Stock_change.py
- Using results from the previous RQ, I check for every date a company was mentioned in the news whether or not its stock value changed with respect to the previous day.
- For this purpose, I pass every row (one for every company with all the headline dates) and open up the stock csv file using the company symbol. I perform similar data manipulation with company.csv as will company_list.csv. This included changing date format and removing non-essential rows. This initial filtering is done using pyspark and Spark-sql.
- After this, i iterate for every date, and fetch the stock values for the headline day and previous day. Then I check if there was a change of more than 5% or not. Changes for all the companies are recorded and as either a zero or one, with one indicating significant change and zero indicating non-significant change.
