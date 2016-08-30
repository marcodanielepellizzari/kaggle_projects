#loading script for file GlobalLandTemperaturesByState.csv
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


#receive a string with country name and return id (integer), if not present in db the country is added in table TEMPERATURE.COUNTRY
#NB this function doesn't COMMIT
def get_idcountry(curs,data,old_data):
	if data['country']==old_data['country']:
		return old_data['idcountry']
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
			return curs.fetchone()[0]


#receive a string with state name and return id (integer), if not present in db the state is added in table TEMPERATURE.STATE
#NB this function doesn't COMMIT
def get_idstate(curs,data,old_data):
	if data['idcountry']!=old_data['idcountry'] or data['state']!=old_data['state']:
		try:
			curs.execute("INSERT INTO TEMPERATURE.STATE(STATE,IDCOUNTRY) VALUES(:1,:2)",(data['state'],data['idcountry'],))
		except cx.DatabaseError as e:
			error, =e.args
			if error.code!=1:
				print(totlines,str(e), line)
		except Exception as e:
			print(totlines,"City",str(e), line)
		finally:
			curs.execute("SELECT IDSTATE from TEMPERATURE.STATE where STATE=:state and IDCOUNTRY=:idcountry",state=data['state'],idcountry=data['idcountry'])
			old_data['idstate']=curs.fetchone()[0]
			old_data['idcountry']=data['idcountry']
			old_data['country']=data['country']
			old_data['state']=data['state']
	return old_data['idstate']

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
old_data={'country':None,'idcountry':None,'state':None,'idstate':None,'latitude':None,'longitude':None}
rows=list()


for line in f:
	#prepare the required data for insertion
	data=dict()
	raw_data=line.strip().split(',')
	data['date']=dt.datetime.strptime(raw_data[0],'%Y-%m-%d')
	data['temperature']=convert_float(raw_data[1])
	data['error']=convert_float(raw_data[2])
	data['state']=raw_data[3]
	data['country']=raw_data[4]
	data['idcountry']=get_idcountry(cur,data,old_data)
	data['idstate']=get_idstate(cur,data,old_data)
	#print(data)
	
	rows.append((data['date'],data['temperature'],data['error'],data['idstate'],))
	totlines+=1
	#insert line
	if totlines%lines_per_insert==0:
		try:
			cur.prepare("INSERT INTO TEMPERATURE.TEMP_STATE(DATA,TEMPERATURE,ERROR,IDSTATE) VALUES(:1,:2,:3,:4)")
			cur.executemany(None,rows)
		#	print(dt.datetime.now().strftime("%H:%M:%S\t"),'Rows inserted:',len(rows))
		except Exception as e:
			print(totlines, "Error:",str(e),"Row not processed:",line)
			print(data)
		finally:
			rows=list()
	if totlines%lines_per_commit==0:
		conn.commit()
		print(dt.datetime.now().strftime("%H:%M:%S\t"),"Rows processed: ",totlines)


if totlines%lines_per_insert!=0:
	try:
		cur.prepare("INSERT INTO TEMPERATURE.TEMP_STATE(DATA,TEMPERATURE,ERROR,IDSTATE) VALUES(:1,:2,:3,:4)")
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
