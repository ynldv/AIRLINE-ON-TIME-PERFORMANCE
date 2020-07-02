import flight_data as fd
import cassandra.cluster


def _insert_flight(flight):
	return f'''INSERT INTO flight(creation_year,tailnum,year,month,day,crs_deptime,date,cancelled,arr_delay,dep_delay,manufacturer) 
	VALUES ({flight.creationYear},'{flight.tailNum}',{flight.year},{flight.month},{flight.day},{flight.crsDepTime},'{flight.date}',
			{flight.cancelled},{flight.arrDelay},{flight.depDelay},'{flight.manufacturer}');
	'''



def _insert_datastream(fname1, fname2, limit=None):
	'''
		from @jbl : insert in database
	'''
	cluster = cassandra.cluster.Cluster()
	session = cluster.connect("ladeveya_projet")

	if limit != None:
		stream = fd.limiteur(fd.read_csv_flight_data(fname1,fname2),limit)
	else:
		stream = fd.read_csv_flight_data(fname1,fname2)
	
	for r in stream:
		session.execute(_insert_flight(r))