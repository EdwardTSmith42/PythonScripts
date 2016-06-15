#python script to run open pos for ap
fileHandle = open('C:\\ap_weekly_04222016.sql', 'r')

yourResult = fileHandle.read().replace('\n',' ').split(';') 

fileHandle.close()
for j,k in enumerate(yourResult):
    yourResult[j]=k.strip()
    if yourResult[j]=='':
        del yourResult[j]

def getLastQuery(theList):
    maxNum=0
    for z,v in enumerate(theList):
 
        if z>maxNum:
            maxNum=z
    return maxNum

import pandas as pd
import pyodbc
import logging
#create database connection
fh=pyodbc.connect(dsn='financial_hast',charset='utf-8',use_unicode=True)
fh.autocommit=True



m=getLastQuery(yourResult)
for  i,y in enumerate(yourResult):

    if i<m:
        query=y
        query=query.format(**locals())
        fh.execute(query)

        print('step '+str(i)+ ' for the query' + ' is done!\n')
        if i==m-1:
            dftemp=pd.read_sql_query(yourResult[m].format(**locals()),fh)
            try:
                df
                df=df.append(dftemp)
                df.to_csv('H:\Adv_Mkt Dept\Generated_Reports\\Weekly\AP_Open.csv',index=False)
                df.to_csv('H:\Adv_Mkt Dept\Generated_Reports\\Jeff\AP_Open.csv',index=False)
            except NameError:
                df=dftemp 
                df.to_csv('H:\Adv_Mkt Dept\Generated_Reports\\Weekly\AP_Open.csv',index=False)
                df.to_csv('H:\Adv_Mkt Dept\Generated_Reports\\Jeff\AP_Open.csv',index=False)