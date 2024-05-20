#from ingest import do_ingest
#from ztf import db, logger
import sys
import requests
import tarfile
import base64
from datetime import datetime, timedelta

import os
import io
import numpy as np

import pandas as pd


import matplotlib
matplotlib.use('cairo')

import matplotlib.pyplot as plt


from pylab import figure, cm
from matplotlib.colors import LogNorm


#import avro

#from avro.datafile import DataFileReader, DataFileWriter
#from avro.io import DatumReader, DatumWriter
import fastavro

from astropy.time import Time
from astropy.io import fits
#import aplpy

from fastavro import writer, parse_schema



import gzip
import re

import sqlite3


from PIL import Image



import GPy

import scipy.signal as signal

		
from astropy.timeseries import TimeSeries
from astropy.timeseries import LombScargle

from scipy.optimize import leastsq
import scipy.stats as stats



from confluent_kafka import Consumer, KafkaException
import os
import sys


import pylab


#os.environ['KRB5_CONFIG'] = '/etc/krb5.conf'




import GPy

import scipy.signal as signal

		
from astropy.timeseries import TimeSeries
from astropy.timeseries import LombScargle

from scipy.optimize import leastsq
import scipy.stats as stats

from astroquery.simbad import Simbad
import astropy.coordinates as coord


from astropy.coordinates import SkyCoord
#from astropy import units as u

import astropy.units as units


import csv
import pickle   #need for serialization
from astroquery.ned import Ned
#from astroquery.ned.core import RemoteServiceError

from urllib3.exceptions import ConnectTimeoutError
from urllib3.exceptions import ReadTimeoutError


import logging




#import and instantiate the StreamingClient:

from antares_client import StreamingClient
import datetime
from antares_client.search import get_by_id, get_by_ztf_object_id

	
	
	
def main():
	
	
	"""
	Use this script as a starting point for streaming alerts from ANTARES.

	Author: YOUR_NAME

	"""
	API_KEY = ""
	API_SECRET = ""


	
	client = StreamingClient(
		topics=["high_amplitude_variable_star_candidate_staging"],
		api_key=API_KEY,
		api_secret=API_SECRET,
		group="Hernitschek"
		)
	

	topic = 'high_amplitude_variable_star_candidate_staging'
	datetimenow = datetime.datetime.now()
	print(datetimenow)


	f = open('logfiles/%s__stream.log' % (topic), "a+")


	dbconn = sqlite3.connect('ztf_alerts_stream.db', isolation_level=None)	
	dbconn.execute('pragma journal_mode=wal;')

	c = dbconn.cursor()

	#### make a loop and keep it running
	while True:
	
		print('try connecting ', datetime.datetime.now())
		f.write('try connecting ' + str( datetime.datetime.now()))
				
		try:
				for topic, locus in client.iter():
					
					print('topic ', topic)
					print('locus ', locus)
					logdate = datetime.datetime.now()

					print("{} received {} on {}".format(logdate,locus, topic))
					locus_id = locus.locus_id
					print('locus_id: ', locus_id)
					jdate = locus.alerts[-1].mjd+2400000.5
						
					t = Time(jdate, format='jd')
							
					
				
					j=np.abs(locus.lightcurve.ant_mjd-locus.alerts[-1].mjd).argmin()

					
					ant_mag_corrected=locus.lightcurve.ant_mag_corrected[j]
					ant_passband=locus.lightcurve.ant_passband[j]
					
					
					print("date: {} {}".format(locus.alerts[-1].mjd, t.isot))
						# MJD field is the time of observation, which is in UTC
						
				
					#logging.info("timestamp %s, locus_id %s " % (datetime.datetime.now(),locus_id))
				
					#f.write("timestamp %s, locus_id %s\n" % (datetime.datetime.now(),locus_id))
					
					
					f.write("timestamp %s, locus_id %s, locus.alerts.alert_id %s\n" % (logdate,locus_id,locus.alerts[-1].alert_id))
					f.flush()
						
					
					print('------')	
					
					

		#if value does not exist in properties: write NaN 


					### write external classification information to database
					c.execute("insert into featuretable values (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",(
					None, logdate, locus.alerts[-1].alert_id, locus_id, locus.ra, locus.dec, locus.alerts[-1].mjd, locus.properties.get("ztf_object_id"),
					locus.properties.get("num_alerts"), locus.properties.get("num_mag_values"), locus.properties.get("brightest_alert_id"),         
					locus.properties.get("brightest_alert_magnitude"), locus.properties.get("brightest_alert_observation_time"),
					locus.properties.get("feature_amplitude_magn_r"), locus.properties.get("feature_anderson_darling_normal_magn_r"),
					locus.properties.get("feature_beyond_1_std_magn_r"), locus.properties.get("feature_beyond_2_std_magn_r"), 
					locus.properties.get("feature_cusum_magn_r"), 
					locus.properties.get("feature_eta_e_magn_r"), locus.properties.get("feature_inter_percentile_range_2_magn_r"),
					locus.properties.get("feature_inter_percentile_range_10_magn_r"), locus.properties.get("feature_inter_percentile_range_25_magn_r"), 
					locus.properties.get("feature_kurtosis_magn_r"), locus.properties.get("feature_linear_fit_slope_magn_r"), locus.properties.get("feature_linear_fit_slope_sigma_magn_r"), 
					locus.properties.get("feature_linear_fit_reduced_chi2_magn_r"), locus.properties.get("feature_linear_trend_magn_r"), locus.properties.get("feature_linear_trend_sigma_magn_r"), 
					locus.properties.get("feature_magnitude_percentage_ratio_40_5_magn_r"), locus.properties.get("feature_magnitude_percentage_ratio_20_5_magn_r"), 
					locus.properties.get("feature_maximum_slope_magn_r"), locus.properties.get("feature_mean_magn_r"), locus.properties.get("feature_median_absolute_deviation_magn_r"), 
					locus.properties.get("feature_percent_amplitude_magn_r"), locus.properties.get("feature_percent_difference_magnitude_percentile_5_magn_r"), 
					locus.properties.get("feature_percent_difference_magnitude_percentile_10_magn_r"), locus.properties.get("feature_median_buffer_range_percentage_10_magn_r"), 
					locus.properties.get("feature_median_buffer_range_percentage_20_magn_r"), locus.properties.get("feature_period_0_magn_r"), 
					locus.properties.get("feature_period_s_to_n_0_magn_r"), locus.properties.get("feature_period_1_magn_r"), locus.properties.get("feature_period_s_to_n_1_magn_r"), 
					locus.properties.get("feature_period_2_magn_r"), locus.properties.get("feature_period_s_to_n_2_magn_r"), locus.properties.get("feature_period_3_magn_r"), 
					locus.properties.get("feature_period_s_to_n_3_magn_r"), locus.properties.get("feature_period_4_magn_r"), locus.properties.get("feature_period_s_to_n_4_magn_r"), 
					locus.properties.get("feature_periodogram_amplitude_magn_r"), locus.properties.get("feature_periodogram_beyond_2_std_magn_r"), 
					locus.properties.get("feature_periodogram_beyond_3_std_magn_r"), locus.properties.get("feature_periodogram_standard_deviation_magn_r"), 
					locus.properties.get("feature_chi2_magn_r"), locus.properties.get("feature_skew_magn_r"), locus.properties.get("feature_standard_deviation_magn_r"), 
					locus.properties.get("feature_stetson_k_magn_r"), locus.properties.get("feature_weighted_mean_magn_r"), locus.properties.get("feature_anderson_darling_normal_flux_r"), 
					locus.properties.get("feature_cusum_flux_r"), locus.properties.get("feature_eta_e_flux_r"), locus.properties.get("feature_excess_variance_flux_r"), 
					locus.properties.get("feature_kurtosis_flux_r"), locus.properties.get("feature_mean_variance_flux_r"), 
					locus.properties.get("feature_chi2_flux_r"), locus.properties.get("feature_skew_flux_r"), 
					locus.properties.get("feature_stetson_k_flux_r"), locus.properties.get("feature_amplitude_magn_g"), 
					locus.properties.get("feature_anderson_darling_normal_magn_g"),
					locus.properties.get("feature_beyond_1_std_magn_g"), locus.properties.get("feature_beyond_2_std_magn_g"), locus.properties.get("feature_cusum_magn_g"), 
					locus.properties.get("feature_eta_e_magn_g"), locus.properties.get("feature_inter_percentile_range_2_magn_g"), locus.properties.get("feature_inter_percentile_range_10_magn_g"), 
					locus.properties.get("feature_inter_percentile_range_25_magn_g"), locus.properties.get("feature_kurtosis_magn_g"), 
					locus.properties.get("feature_linear_fit_slope_magn_g"), 
					locus.properties.get("feature_linear_fit_slope_sigma_magn_g"), locus.properties.get("feature_linear_fit_reduced_chi2_magn_g"), 
					locus.properties.get("feature_linear_trend_magn_g"), locus.properties.get("feature_linear_trend_sigma_magn_g"), 
					locus.properties.get("feature_magnitude_percentage_ratio_40_5_magn_g"), locus.properties.get("feature_magnitude_percentage_ratio_20_5_magn_g"), 
					locus.properties.get("feature_maximum_slope_magn_g"), locus.properties.get("feature_mean_magn_g"), locus.properties.get("feature_median_absolute_deviation_magn_g"), 
					locus.properties.get("feature_percent_amplitude_magn_g"), locus.properties.get("feature_percent_difference_magnitude_percentile_5_magn_g"), 
					locus.properties.get("feature_percent_difference_magnitude_percentile_10_magn_g"), locus.properties.get("feature_median_buffer_range_percentage_10_magn_g"), 
					locus.properties.get("feature_median_buffer_range_percentage_20_magn_g"), locus.properties.get("feature_period_0_magn_g"), locus.properties.get("feature_period_s_to_n_0_magn_g"), 
					locus.properties.get("feature_period_1_magn_g"), locus.properties.get("feature_period_s_to_n_1_magn_g"), locus.properties.get("feature_period_2_magn_g"), 
					locus.properties.get("feature_period_s_to_n_2_magn_g"), locus.properties.get("feature_period_3_magn_g"), locus.properties.get("feature_period_s_to_n_3_magn_g"), 
					locus.properties.get("feature_period_4_magn_g"), locus.properties.get("feature_period_s_to_n_4_magn_g"), locus.properties.get("feature_periodogram_amplitude_magn_g"), 
					locus.properties.get("feature_periodogram_beyond_2_std_magn_g"), locus.properties.get("feature_periodogram_beyond_3_std_magn_g"), 
					locus.properties.get("feature_periodogram_standard_deviation_magn_g"), locus.properties.get("feature_chi2_magn_g"), locus.properties.get("feature_skew_magn_g"), 
					locus.properties.get("feature_standard_deviation_magn_g"), locus.properties.get("feature_stetson_k_magn_g"), locus.properties.get("feature_weighted_mean_magn_g"), 
					locus.properties.get("feature_anderson_darling_normal_flux_g"), locus.properties.get("feature_cusum_flux_g"), locus.properties.get("feature_eta_e_flux_g"), 
					locus.properties.get("feature_excess_variance_flux_g"), locus.properties.get("feature_kurtosis_flux_g"), locus.properties.get("feature_mean_variance_flux_g"), 
					locus.properties.get("feature_chi2_flux_g"), locus.properties.get("feature_skew_flux_g"), locus.properties.get("feature_stetson_k_flux_g"), 
					locus.properties.get("anomaly_score"), locus.properties.get("anomaly_mask"), locus.properties.get("anomaly_type"), locus.properties.get("is_corrected"),
					ant_mag_corrected, ant_passband) )
									
									
					dbconn.commit()							
									

		except:
			print('an exception has occurred')
			pass

				
				
				
	f.close()
	client.close()
		
	
	dbconn.close()




#####

if __name__ == "__main__":
    main()  
  
  
  
  
            
            
            
