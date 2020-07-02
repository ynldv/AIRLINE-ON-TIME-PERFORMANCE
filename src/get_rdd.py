import csv
import datetime
import itertools
import collections
import math
import numpy as np
import scipy as sp
import scipy.stats
import scipy.interpolate
import matplotlib.pyplot as plt
import pyspark

import flight_data as fd



limiteur = lambda generator, limit: (data for _, data in zip(range(limit), generator))



def get_RDD(fname, dic, limit=None, sc=None, numSlices=None):
    '''
        from @jbl
    '''
    if sc is None:
        sparkconf = pyspark.SparkConf()
        sparkconf.set('spark.port.maxRetries', 128)
        sc = pyspark.SparkContext(conf=sparkconf)
    if numSlices is None:
        numSlices = 1000
    if limit == None:
    	D = sc.parallelize(fd.read_csv_flight_w_seat(fname, dic), numSlices=numSlices)
    else:
    	D = sc.parallelize(limiteur(fd.read_csv_flight_w_seat(fname, dic),limit), numSlices=numSlices)
    return sc, D