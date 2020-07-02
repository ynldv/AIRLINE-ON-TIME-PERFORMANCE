import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np

import get_rdd

week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

def compute_day(day):
	if day == 1:
		return 'Monday'
	elif day == 2:
		return 'Tuesday'
	elif day == 3:
		return 'Wednesday'
	elif day == 4:
		return 'Thursday'
	elif day == 5:
		return 'Friday'
	elif day == 6:
		return 'Saturday'
	else:
		return 'Sunday'

def compute_hour(hour):
	'''
		Return the hour from hhmm format
	'''
	if len(str(hour)) == 4:
		return int(str(hour)[:2])
	else:
		return int('0'+str(hour)[0])


def get_nb_passengers_per_day(data):
	value = (
		data.map(lambda x: (compute_day(x.dayOfWeek), x.seat))
		.reduceByKey(lambda x,y: x+y)
		.collect()
		)
	return value


def get_nb_passengers_per_day_hour(data):
	value = (
		data.map(lambda x: ((compute_day(x.dayOfWeek),compute_hour(x.crsDepTime)), x.seat))
		.reduceByKey(lambda x,y: x+y)
		.collect()
		)
	return value


def get_nb_passengers_per_month(data):
	value = (
		data.map(lambda x: (x.month, x.seat))
		.reduceByKey(lambda x,y: x+y)
		.collect()
		)
	return value


def display_per_day_hour(data):
	'''
		Display barplot from get_nb_passengers_per_day_hour map reduce function
	'''
	value = get_nb_passengers_per_day_hour(data)
	fig, ax = plt.subplots()
	filt = [[l[0][0],l[0][1],l[1]] for l in value]
	df = pd.DataFrame(data=filt, columns=['Day','Hour','Passengers']).sort_values(by=['Day','Hour'])
	print(df)
	sns.barplot(x='Hour',y='Passengers',hue='Day', data=df)	
	ax.legend(loc='upper center', ncol=4, bbox_to_anchor=(0.5, 1.1), fontsize='small', fancybox=True, shadow=True)
	fig.savefig('../img/passengers_per_hour.png')


def display_per_month(data):
		'''
		Display barplot from get_nb_passengers_per_month map reduce function
	'''
	value = get_nb_passengers_per_month(data)
	fig, ax = plt.subplots(1)
	df = pd.DataFrame(data=value, columns=['Month','Passengers'])
	print(df.to_latex())
	sns.barplot(x='Month', y='Passengers', data=df, palette=sns.color_palette("Blues_d", n_colors=12))
	fig.savefig('../img/passengers_per_month.png')


	