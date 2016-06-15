#python script to run items to change to return 5
#emails file at the end
fileHandle = open('//winhome/home/smithr/Python Scripts/SQL/rc_updates.sql', 'r')

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
                df.to_csv('H:\Adv_Mkt Dept\Generated_Reports\\Weekly\\rc_to_5.csv',index=False)

            except NameError:
                df=dftemp 
                df.to_csv('H:\Adv_Mkt Dept\Generated_Reports\\Weekly\\rc_to_5.csv',index=False)
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEBase import MIMEBase
from email import encoders 
 
fromaddr = "edward.smith@gohastings.com"
toaddr = ['edward.smith@gohastings.com','khan.boillat@gohastings.com']
msg = MIMEMultipart()
msg['From'] = fromaddr
msg['To'] = ', '.join(toaddr)
msg['Subject'] = "Daily RC to Rtn 5 report"
 
body = """The Daily update to return 5 report is attached. Please update the attached items to return 5.
Please let me know if you have any questions.

Ed"""
msg.attach(MIMEText(body, 'plain'))
 
filename = ['rc_to_5.csv']
for f in filename:

	attachment = open("H:\Adv_Mkt Dept\Generated_Reports\\Weekly\\"+f, "rb")
 
	part = MIMEBase('application', 'octet-stream')
	part.set_payload((attachment).read())
	encoders.encode_base64(part)
	part.add_header('Content-Disposition', "attachment; filename= %s" % f)
 
	msg.attach(part)
 
 
 
 
server = smtplib.SMTP('exch2010.hasting.com')

text = msg.as_string()
server.sendmail(fromaddr, toaddr, text)
server.quit()
