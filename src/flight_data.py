import collections
import csv
import datetime
import time


#
#	Lecture CSV
#

limiteur = lambda generator, limit: (data for _, data in zip(range(limit), generator))


#
#	Q1 - Do older planes suffer more delays ?
#

Flight = collections.namedtuple(
	"Flight",
	("creationYear","tailNum","year","month","day","date","crsDepTime","cancelled","arrDelay","depDelay","manufacturer")
)


Plane = collections.namedtuple(
	"Plane",
	("tailnum","manufacturer","year")
)



def read_csv_flight_data(fname1,fname2):
	'''
		Read csv file 2005.csv, plane-data.csv and return Flight namedtuple
	'''
	dico = dico_csv_plane_data(fname2)
	with open(fname1) as f:
		for row in csv.DictReader(f):
			if int(row["Cancelled"]) == 0 and str(row["ArrDelay"]) != 'NA' and str(row["DepDelay"]) != 'NA':
				y = int(row["Year"])
				m = int(row["Month"])
				d = int(row["DayofMonth"])
				date = datetime.datetime(y,m,d)
				time = int(row["CRSDepTime"])
				tailNum = str(row["TailNum"])
				cancelled = int(row["Cancelled"])
				arrDelay = int(row["ArrDelay"])
				depDelay = int(row["DepDelay"])
				if dico.get(tailNum) == None:
					manufacturer = 'Unknown'
					creationYear = 0
				else:
					manufacturer = dico[tailNum][0]
					creationYear = dico[tailNum][1]
				yield Flight(creationYear,tailNum,y,m,d,date,time,cancelled,arrDelay,depDelay,manufacturer)


def dico_csv_plane_data(fname):
	'''
		Return a dictionnary from plane-data.csv {tailnum:(manufacturer,year)}
	'''
	dico = {}
	with open(fname) as f:
		for row in csv.DictReader(f):
			tailnum = str(row["tailnum"])
			if row["year"] == None or row["year"] == 'None':
				dico[str(row["tailnum"])] = ('Unknown',0)
			else:
				dico[str(row["tailnum"])] = (str(row["manufacturer"]),int(row["year"]))

	return dico


def compute_time(n):
	'''
		Return the iteration time of read_csv_flight_data
	'''
	start_time = time.time()
	gen = limiteur(read_csv_flight_data('../2005.csv','../plane-data.csv'),n)
	for i in gen:
		pass
	print(f'--- {time.time()-start_time} seconds ---')


def check_NA_flight(fname1):
	with open(fname1) as f:
		i = 0
		for row in csv.DictReader(f):
			if int(row["Cancelled"]) == 0 and (str(row["ArrDelay"]) == 'NA' or str(row["DepDelay"]) == 'NA'):
				i += 1
	print(i)


def check_incomplete_plane(fname):
	with open(fname) as f:
		i = 0
		for row in csv.DictReader(f):
			if row["year"] == None or row["year"] == 'None':
				i +=1
	print(i)



#
#	Q2 - How does the number of people flying between different location change overtime ?
#

Flight_w_seat = collections.namedtuple(
	"Flight_w_seat",
	("year","month","day","dayOfWeek","crsDepTime","tailNum","seat","cancelled")
)


# fname1 = plane-seats.csv, fname = plane-data.csv
def dico_model_seat(fname1,fname2):
	'''
		Return dictionnary {model: n_seats}
	'''
	dic = {}
	model = []
	for row in csv.DictReader(open(fname2)):
		m = str(row["model"])
		if m not in model:
			model.append(m)
	for m in model:
		for s in csv.DictReader(open(fname1)):
			if m == str(s["MODEL"]).strip():
				dic[m] = int(s["NO-SEATS"])
				break
	return dic

# fname1 = plane-data.csv, fname2 = plane-seats.csv
def dico_plane_seat(fname1,fname2):
	'''
		Return dictionnary {tailnum:n_seats}
	'''
	dic = dico_model_seat(fname2,fname1)
	dico_plane_seat = {}
	with open(fname1) as f:
		for row in csv.DictReader(f):
			dico_plane_seat[str(row["tailnum"])] = dic.get(str(row["model"]))
	return dico_plane_seat

# fname1 = 2005.csv, dic = dicplo_plane_seat(plane-data.csv,plane-seats.csv)
def read_csv_flight_w_seat(fname,dic):
	'''
		Read 2005.csv and dic then return generator of Flight_w_seat
	'''
	with open(fname) as f:
		for row in csv.DictReader(f):
			if int(row["Cancelled"]) == 0:
				year = int(row["Year"])
				month = int(row["Month"])
				day = int(row["DayofMonth"])
				dayOfWeek = int(row["DayOfWeek"])
				crs = int(row["CRSDepTime"])
				cancelled = int(row["Cancelled"])
				tailnum = str(row["TailNum"])

				mean = 0
				it = 0
				for i,j in dic.items():
					if j != None:
						mean += j
						it += 1
				mean = round(mean/it)
				
				if dic.get(tailnum) == None:
					dic[tailnum] = mean

				yield Flight_w_seat(year,month,day,dayOfWeek,crs,tailnum,dic.get(tailnum),cancelled)
