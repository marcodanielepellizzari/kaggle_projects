#loading script for file GlobalLandTemperaturesByCity.csv
#header line must be removed from file


import cx_Oracle as cx
import datetime as dt
import os
import sys

#source and target parameters
file_name=sys.argv[1]

username='temperature'
password='temperature'
dbserver='srvoratest'
port=1521
orasid='ORATEST'

os.environ["NLS_LANG"] = "AMERICAN_AMERICA.WE8MSWIN1252"



#execution parameters
lines_per_commit=10000
lines_per_insert=100

#function to properly treat missing data
def convert_float(string):
	try:
		return float(string)
	except:
		return None



#receive a string with city name and return id (integer), if not present in db the country is added in table TEMPERATURE.COUNTRY
#NB this function doesn't COMMIT
def get_idcountry(curs,data,old_data):
	if data['country']==old_data['country']:
		pass
	else:
		try:
			curs.execute("INSERT INTO TEMPERATURE.COUNTRY(COUNTRY) VALUES (:1)",(data['country'],))
                except cx.DatabaseError as e:
                        error, =e.args
                        if error.code!=1:
                                print(totlines,"Country",str(e), line)
		except Exception as e:
			print(totlines,str(e), line)
			pass
		finally:
			curs.execute("SELECT IDCOUNTRY FROM TEMPERATURE.COUNTRY WHERE COUNTRY=:1",(data['country'],))
			old_data['idcountry']=curs.fetchone()[0]
			old_data['country']=data['country']
	return old_data['idcountry']


#open source
f=open(file_name,'r')
print(dt.datetime.now().strftime("%H:%M:%S\t"),"Opened file ",file_name )


#open target
dsn_tns=cx.makedsn(dbserver,port,orasid)
conn=cx.connect(username,password,dsn_tns)
cur=conn.cursor()
print(dt.datetime.now().strftime("%H:%M:%S\t"),"Opened connection to db ", orasid)



#working variables
totlines=0
old_data={'country':None,'idcountry':None,'city':None,'idcity':None,'latitude':None,'longitude':None}
rows=list()


for line in f:
	#prepare the required data for insertion
	data=dict()
	raw_data=line.strip().split(',')
	data['date']=dt.datetime.strptime(raw_data[0],'%Y-%m-%d')
	data['temperature']=convert_float(raw_data[1])
	data['error']=convert_float(raw_data[2])
	data['country']=raw_data[3]
	data['idcountry']=get_idcountry(cur,data,old_data)
	#print(data)
	
	rows.append((data['date'],data['temperature'],data['error'],data['idcountry'],))
	totlines+=1
	#insert line
	if totlines%lines_per_insert==0:
		try:
			cur.prepare("INSERT INTO TEMPERATURE.TEMP_COUNTRY(DATA,TEMPERATURE,ERROR,IDCOUNTRY) VALUES(:1,:2,:3,:4)")
			cur.executemany(None,rows)
		#	print(dt.datetime.now().strftime("%H:%M:%S\t"),'Rows inserted:',len(rows))
		except Exception as e:
			print(totlines, "Error:",str(e),"Row not processed:",line)
			pass
		finally:
			rows=list()
	if totlines%lines_per_commit==0:
		conn.commit()
		print(dt.datetime.now().strftime("%H:%M:%S\t"),"Rows processed: ",totlines)


if totlines%lines_per_insert!=0:
	try:
		cur.prepare("INSERT INTO TEMPERATURE.TEMP_COUNTRY(DATA,TEMPERATURE,ERROR,IDCOUNTRY) VALUES(:1,:2,:3,:4)")
		cur.executemany(None,rows)
		print(dt.datetime.now().strftime("%H:%M:%S\t"),'Rows inserted:',len(rows))
	except Exception as e:
		print(totlines, "Error:",str(e),"Row not processed:",line)
conn.commit()

print(dt.datetime.now().strftime("%H:%M:%S\t"),"Rows processed: ",totlines)
print(dt.datetime.now().strftime("%H:%M:%S\t"),"All rows have been processed")


#close source
f.close()
print(dt.datetime.now().strftime("%H:%M:%S\t"),"File closed ", file_name)

#close target
cur.close()
conn.close()
print("Closed connection to db ", orasid)
print("The end")
