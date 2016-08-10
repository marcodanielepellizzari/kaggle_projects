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

#latitude and longitude will be stored in db as float
def convert_latitude(lat):
	value=float(lat.strip()[:len(lat)-1])
	if lat[len(lat)-1]=='S':
		return value*(-1)
	else:
		return value

def convert_longitude(lon):
	value=float(lon.strip()[:len(lon)-2])
	if lon[len(lon)-1]=='W':
		return value*(-1)
	else:
		return value

#function to properly treat missing data
def convert_float(string):
	try:
		return float(string)
	except:
		return None



#receive a string with city name and return id (integer), if not present in db the city is added in table TEMPERATURE.COUNTRY
#NB this function doesn't COMMIT
def get_idcountry(curs,data,old_data):
	if data['country']==old_data['country']:
		return old_data['idcountry']
	else:
		try:
			curs.execute("INSERT INTO TEMPERATURE.COUNTRY(COUNTRY) VALUES (:1)",(data['country'],))
		except cx.DatabaseError as e:
			if e.code!=1:
				print(totlines,str(e), line)
		except Exception as e:
			print(totlines,str(e), line)
			pass
		finally:
			curs.execute("SELECT IDCOUNTRY FROM TEMPERATURE.COUNTRY WHERE COUNTRY=:1",(data['country'],))
			return curs.fetchone()[0]


#receive a string with city name and return id (integer), if not present in db the city is added in table TEMPERATURE.CITY
#NB this function doesn't COMMIT
def get_idcity(curs,data,old_data):
	if data['idcountry']!=old_data['idcountry'] or data['city']!=old_data['city'] or data['latitude']!=old_data['latitude'] or data['longitude']!=old_data['longitude']:
		try:
			curs.execute("INSERT INTO TEMPERATURE.CITY(CITY,IDCOUNTRY,LATITUDE,LONGITUDE) VALUES(:city,:idcountry,:latitude,:longitude)",city=data['city'],idcountry=data['idcountry'], latitude=data['latitude'],longitude=data['longitude'])
#		except cx.DatabaseError as e:
#			if e.code!=1:
#				print(totlines,str(e), line)
		except Exception as e:
			print(totlines,str(e), line)
		finally:
			curs.execute("SELECT IDCITY from TEMPERATURE.CITY where CITY=:city and IDCOUNTRY=:idcountry and LATITUDE=:latitude and LONGITUDE=:longitude",city=data['city'],idcountry=data['idcountry'], latitude=data['latitude'],longitude=data['longitude'])
			old_data['idcity']=curs.fetchone()[0]
			old_data['idcountry']=data['idcountry']
			old_data['city']=data['city']
			old_data['latitude']=data['latitude']
			old_data['longitude']=data['longitude']
	return old_data['idcity']

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
	raw_data=line.split(',')
	data['date']=dt.datetime.strptime(raw_data[0],'%Y-%m-%d')
	data['temperature']=convert_float(raw_data[1])
	data['error']=convert_float(raw_data[2])
	data['city']=raw_data[3]
	data['country']=raw_data[4]
	data['latitude']=convert_latitude(raw_data[5])
	data['longitude']=convert_longitude(raw_data[6])
	data['idcountry']=get_idcountry(cur,data,old_data)
	data['idcity']=get_idcity(cur,data,old_data)
	#print(data)
	
	rows.append((data['date'],data['temperature'],data['error'],data['idcity'],))
	totlines+=1
	#insert line
	if totlines%lines_per_insert==0:
		try:
			cur.prepare("INSERT INTO TEMPERATURE.TEMP_CITY(DATA,TEMPERATURE,ERROR,IDCITY) VALUES(:1,:2,:3,:4)")
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
		cur.prepare("INSERT INTO TEMPERATURE.TEMP_CITY(DATA,TEMPERATURE,ERROR,IDCITY) VALUES(:1,:2,:3,:4)")
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
