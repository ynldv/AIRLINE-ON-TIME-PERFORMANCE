import collections
import csv
import datetime
import cassandra.cluster
import textwrap
import numpy as np
import json 
import decimal
import functools
import matplotlib.pyplot as plt
import time

import flight_data as fd
import feed_cassandra


PlaneCumulatedDelay = collections.namedtuple(
	"PlaneCumulatedDelay",
	("tailnum","issue_date","delay")
)

class FlightTrip:
	def __init__(self):
		self._cluster = cassandra.cluster.Cluster()
		self._session = self._cluster.connect('ladeveya_projet')


	def get_flight_one_year(self,year):
		'''
			Return all values where creation_year {pk} = year
		'''
		return f'''SELECT * FROM flight WHERE creation_year={year};'''


	def get_flight(self,yearInf,yearSup):
		'''
			Return generator of all flights for plane from yearInf to yearSup
		'''
		if yearSup > 2005:
			yearSup = 2005
		for i in range(yearInf,yearSup+1):
			for s in self._session.execute(self.get_flight_one_year(i)):
				if s.cancelled != 1:
					yield fd.Flight(s.creation_year,s.tailnum,s.year,s.month,s.day,s.date,s.crs_deptime,s.cancelled,s.arr_delay,s.dep_delay,s.manufacturer)


	def get_cumulated_delay_by_plane(self,yearInf,yearSup):
		'''
			Return the mean delay by flight for each plane
		'''
		start_time = time.time()
		# [tailNum, creationYear, delay, numberOfFlights]
		listTailNum = []
		for i in self.get_flight(yearInf,yearSup):
			if [i.tailNum,i.creationYear,0,0] not in listTailNum:
				listTailNum.append([i.tailNum,i.creationYear,0,0])

		for i in self.get_flight(yearInf,yearSup):
			for j in listTailNum:
				if j[0] == i.tailNum:
					j[2] += i.arrDelay
					j[3] += 1

		for i in listTailNum:
			if i[2] < 0 or i[3] == 0:
				i[2] = 0
			else:
				i[2] = i[2]/i[3]

		print(f'--- {time.time()-start_time} seconds ---')

		return listTailNum


	def get_cumulated_delay_by_year(self,yearInf,yearSup):
		'''
			Return the mean delay by year for one flight
		'''
		start_time = time.time()
		# [year, delay, numberOfFlights]
		delay_by_year = [[i,0,0] for i in range(yearInf,yearSup+1) if i<=2005]
		
		for i in self.get_flight(yearInf,yearSup):
			for j in delay_by_year:
				if i.creationYear == j[0]:
					j[1] += i.arrDelay
					j[2] += 1

		for i in delay_by_year:
			if i[1] < 0 or i[2] == 0:
				i[1] = 0
			else:
				i[1] = i[1]/i[2]

		print(f'--- {time.time()-start_time} seconds ---')

		return delay_by_year


	def get_cumulated_delay_by_year_manufacturer(self,yearInf,yearSup):
		'''
			Return the mean delay by year and manufacturer for one flight
		'''
		start_time = time.time()
		#[creationYear, manufacturer, delay, count]
		delay = []
		for i in self.get_flight(yearInf,yearSup):
			if [i.creationYear, i.manufacturer, 0, 0] not in delay:
				delay.append([i.creationYear, i.manufacturer, 0, 0])

		for i in self.get_flight(yearInf, yearSup):
			for j in delay:
				if j[0] == i.creationYear  and j[1] == i.manufacturer:
					j[2] += i.arrDelay
					j[3] += 1

		for i in delay:
			if i[2] < 0 or i[3] == 0:
				i[2] = 0
			else:
				i[2] = i[2]/i[3]

		print(f'--- {time.time()-start_time} seconds ---')

		return delay


	def display_by_year(self, arr):
		'''
			Display barplot of get_cumulated_delay_by_year
		'''
		year = [x[0] for x in arr if (x[0]!=0 and x[2]!=0)]
		delay = [x[1] for x in arr if (x[0]!=0 and x[2]!=0)]

		plt.bar(year,delay)
		plt.xlabel('Year')
		plt.ylabel('Mean delay (min)')
		plt.xticks(np.arange(min(year), max(year)+1, 1.0),rotation=45)
		plt.title('Cumulated delay by year')
		plt.savefig('../img/delay_by_year.png')


	def display_by_year_manufacturer(self, arr):
		'''
			Display barplot of get_cumulated_delay_by_year_manufacturer
		'''
		year = np.unique([x[0] for x in arr if (x[0]!=0 and x[3]!=0)])
		manufacturer = np.unique([x[1] for x in arr])
		for i in range(len(manufacturer)):
			delay = [(x[0],x[2]) for x in arr if (x[1]==manufacturer[i] and x[0]!=0 and x[3]!=0)]
			subyear = [x[0] for x in delay]
			plt.figure(i)
			plt.bar(subyear,[x[1] for x in delay], label=manufacturer[i])
			plt.xticks(subyear, rotation=45)
			plt.legend(loc='upper right', fontsize=5)
			plt.savefig(f'../img/delay_by_year_{str(manufacturer[i]).replace(" ","_").replace("/","_")}.png')
