# importing all required libraries
import requests,os,datetime,sqlite3
import pandas as pd
from pandas.io.json import json_normalize

## Fetch key from an external config file

import os
path = os.getenv('USERPROFILE')
filename = path+'\\fixer_access_key.txt'
file=open(filename,'r')
for line in file:
    access_key = line.split('=')[1]


class webservice:
    def __init__(self,access_key):
        self.access_key = access_key
    
    def exchangerate_webservice(self,url):
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            dataset = json_normalize(data)
            return dataset
    
    def find_average_rate(self,currency,data):
        data = data.groupby(['base']).mean()['rates.'+currency].reset_index()
        return data
    
    def convert_to_euro(self,currency,amount):
        convert_url="https://data.fixer.io/api/convert?access_key={0}&from={1}&to=EUR&amount={2}".format(self.access_key,currency,amount)    
        data = exchangerate_webservice(convert_url)
        return data
    
class writetoDB:
    def __init__(self,db,table,method,data):
        self.db = db
        self.table =  table
        self.method = method
        self.data = data
    def writetosqlite(self):
        conn = sqlite3.connect(self.db)
        self.data.to_sql(self.table,con=conn,if_exists=self.method,index=False)
        

def main():
    base_curr = input("Type 3 digit base currency code?(Only EUR supported by access key)  ") 
    current_url = "http://data.fixer.io/api/latest?access_key={0}&base={1}".format(access_key,base_curr) 

    wb = webservice(access_key)

    current_rates = wb.exchangerate_webservice(current_url)
    current_rates['historical']=False

    base = datetime.datetime.today()
    day_range = int(input("Enter days for which history data has to be fetched eg)100,200 etc : ")) #2 years require 730 api calls therefore putting only 100 now
    date_list = [(base - datetime.timedelta(days=x)).strftime('%Y-%m-%d') for x in range(1, day_range)]
    current_data = current_rates.copy()

#have to call in loop as current subsription does not allow to call time series method 
#which would have fetched all requried historical records in a single call
    for day in date_list:
        historical_url = "http://data.fixer.io/api/{0}?access_key={1}&base={2}".format(day,access_key,base_curr)
        historical_rates = wb.exchangerate_webservice(historical_url)
        current_data = pd.concat([current_data,historical_rates],sort=True)
    

    wdb = writetoDB('example.db','currency_rate','replace',current_data)
    wdb.writetosqlite()

#### Calculate average currency rate over a period
    avg=input('Please enter Y/N to calculate average conversion rate of a currency with Euro: ')
    if avg=='Y':
        print("Base curreny is {0}, please enter date range for which average conversation rate should be calculated".format(base_curr))
        start_date =  input("Enter start date in YYYY-MM-DD format: ")
        end_date =  input("Enter end date in YYYY-MM-DD format: ")
        currency = input("Enter 3 digit currency code (base currency is {0}) : ".format(base_curr))
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        current_data['date'] =  pd.to_datetime(current_data['date'], format='%Y-%m-%d')
        data = current_data[(current_data['date'] >= start_date) & (current_data['date'] <= end_date)]
        data = wb.find_average_rate(currency,data)
        print(data)
    else:
        pass 
    return current_data

if __name__ == '__main__':

    historical_data = main()
    csv_flag = input('please press Y/N to write historical data to a csv file: ')
    if csv_flag=='Y':
        historical_data.to_csv('historical_conversion_rate.csv',index=False)

