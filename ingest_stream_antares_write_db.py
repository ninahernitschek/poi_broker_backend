#
#
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


from astropy.time import Time
from astropy.io import fits
#import aplpy



import sqlite3


#import GPy

#import scipy.signal as signal

		
#from astropy.timeseries import TimeSeries
#from astropy.timeseries import LombScargle

#from scipy.optimize import leastsq
#import scipy.stats as stats



#from confluent_kafka import Consumer, KafkaException
import os
import sys

#import pylab



#import GPy
	
	
	
#from astroquery.simbad import Simbad
#import astropy.coordinates as coord


from astropy.coordinates import SkyCoord

import astropy.units as u


#import csv
#import pickle   #need for serialization
#from astroquery.ned import Ned
#from astroquery.ned.core import RemoteServiceError

#from urllib3.exceptions import ConnectTimeoutError
#from urllib3.exceptions import ReadTimeoutError


import logging




#import and instantiate the StreamingClient:

from antares_client import StreamingClient
import datetime
#from antares_client.search import get_by_id, get_by_ztf_object_id

from datetime import datetime

from antares_client.search import get_by_id

import numpy as np

import astropy.units as u

from astropy.coordinates import SkyCoord




import pandas as pd


import timeit

from multiprocessing import Pool



import P4J
import mhps

import celerite2
from celerite2 import terms
from scipy.optimize import minimize

import math


from scipy import stats
from statsmodels.tsa import stattools
from scipy.interpolate import interp1d
from scipy.stats import chi2
from scipy.optimize import minimize, minimize_scalar

import csv



from sklearn.ensemble import GradientBoostingClassifier

from xgboost import XGBClassifier


# (quasi-)periodic:
# TESS: ACEP, ACEPS, AM, BY, CEP, CTTS_ROT, CV, CWA, CWB, DCEP, DCEPS, DSCT, E, EA, EB, ELL, EW, HADS, M, LPV, 
# ROT, RR, RRAB, RRC, RRC_BL, RRD, RS, SR, SRA, SRB, SRS, TTS_ROT
# 
# stochastic:
# TESS: BLLAC, CTTS, IN, INS, L, UG, UGSS, UV, UVN
# 
# transient: 
# TESS: EXOR, NL_VY, RCB


import os

from sklearn.preprocessing import LabelEncoder


import imblearn
from imblearn.ensemble import BalancedRandomForestClassifier

from imblearn.ensemble import BalancedBaggingClassifier


from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split


import pickle

from sklearn.preprocessing import StandardScaler

def classify(alert_id):

	print('in classify')
	model_filename = 'xgboost_model.pickle'
	trained_classifier = pickle.load(open(model_filename,'rb'))
	print('model loaded')

	current_dirs_parent = os.path.dirname(os.getcwd())
	db_path = current_dirs_parent + '/_broker_db/ztf_alerts_stream.db'
		
	dbconn = sqlite3.connect(db_path, isolation_level=None)	

	
	c_2 = dbconn.cursor()
	
	sql_query = "SELECT locus_id,alert_id,date_alert_mjd,feature_amplitude_magn_r,feature_anderson_darling_normal_magn_r,feature_beyond_1_std_magn_r,\
	feature_beyond_2_std_magn_r,feature_cusum_magn_r,feature_eta_e_magn_r,feature_inter_percentile_range_2_magn_r,\
	feature_inter_percentile_range_10_magn_r,feature_inter_percentile_range_25_magn_r,feature_kurtosis_magn_r,feature_linear_fit_slope_magn_r,\
	feature_linear_fit_slope_sigma_magn_r,feature_linear_fit_reduced_chi2_magn_r,feature_linear_trend_magn_r,\
	feature_linear_trend_sigma_magn_r,feature_magnitude_percentage_ratio_40_5_magn_r,feature_magnitude_percentage_ratio_20_5_magn_r,\
	feature_maximum_slope_magn_r,feature_mean_magn_r,feature_median_absolute_deviation_magn_r,feature_percent_amplitude_magn_r,\
	feature_percent_difference_magnitude_percentile_5_magn_r,feature_percent_difference_magnitude_percentile_10_magn_r,\
	feature_median_buffer_range_percentage_10_magn_r,feature_median_buffer_range_percentage_20_magn_r,feature_period_0_magn_r,\
	feature_period_s_to_n_0_magn_r,feature_period_1_magn_r,feature_period_s_to_n_1_magn_r,feature_period_2_magn_r,\
	feature_period_s_to_n_2_magn_r,feature_period_3_magn_r,feature_period_s_to_n_3_magn_r,feature_period_4_magn_r,\
	feature_period_s_to_n_4_magn_r,feature_periodogram_amplitude_magn_r,feature_periodogram_beyond_2_std_magn_r,\
	feature_periodogram_beyond_3_std_magn_r,feature_periodogram_standard_deviation_magn_r,feature_chi2_magn_r,\
	feature_skew_magn_r,feature_standard_deviation_magn_r,feature_stetson_k_magn_r,feature_weighted_mean_magn_r,\
	feature_anderson_darling_normal_flux_r,feature_cusum_flux_r,feature_eta_e_flux_r,feature_excess_variance_flux_r,\
	feature_kurtosis_flux_r,feature_mean_variance_flux_r,feature_chi2_flux_r,feature_skew_flux_r,feature_stetson_k_flux_r,\
	feature_amplitude_magn_g,feature_anderson_darling_normal_magn_g,feature_beyond_1_std_magn_g,feature_beyond_2_std_magn_g,\
	feature_cusum_magn_g,feature_eta_e_magn_g,feature_inter_percentile_range_2_magn_g,feature_inter_percentile_range_10_magn_g,\
	feature_inter_percentile_range_25_magn_g,feature_kurtosis_magn_g,feature_linear_fit_slope_magn_g,feature_linear_fit_slope_sigma_magn_g,\
	feature_linear_fit_reduced_chi2_magn_g,feature_linear_trend_magn_g,feature_linear_trend_sigma_magn_g,\
	feature_magnitude_percentage_ratio_40_5_magn_g,feature_magnitude_percentage_ratio_20_5_magn_g,feature_maximum_slope_magn_g,\
	feature_mean_magn_g,feature_median_absolute_deviation_magn_g,feature_percent_amplitude_magn_g,\
	feature_percent_difference_magnitude_percentile_5_magn_g,feature_percent_difference_magnitude_percentile_10_magn_g,\
	feature_median_buffer_range_percentage_10_magn_g,feature_median_buffer_range_percentage_20_magn_g,feature_period_0_magn_g,\
	feature_period_s_to_n_0_magn_g,feature_period_1_magn_g,feature_period_s_to_n_1_magn_g,feature_period_2_magn_g,\
	feature_period_s_to_n_2_magn_g,feature_period_3_magn_g,feature_period_s_to_n_3_magn_g,feature_period_4_magn_g,feature_period_s_to_n_4_magn_g,\
	feature_periodogram_amplitude_magn_g,feature_periodogram_beyond_2_std_magn_g,feature_periodogram_beyond_3_std_magn_g,\
	feature_periodogram_standard_deviation_magn_g,feature_chi2_magn_g,feature_skew_magn_g,feature_standard_deviation_magn_g,\
	feature_stetson_k_magn_g,feature_weighted_mean_magn_g,feature_anderson_darling_normal_flux_g,feature_cusum_flux_g,feature_eta_e_flux_g,\
	feature_excess_variance_flux_g,feature_kurtosis_flux_g,feature_mean_variance_flux_g,feature_chi2_flux_g,feature_skew_flux_g,\
	feature_stetson_k_flux_g,anomaly_score,anomaly_mask,anomaly_type,g_r_max,g_r_mean,best_period,\
	best_period_significance,best_period_g,best_period_r,best_period_i,power_rate_0p5,power_rate_0p333,power_rate_0p25,power_rate_2,\
	power_rate_3,power_rate_4,mhps_ratio_g,mhps_ratio_R,mhps_ratio_i,mhps_low_g,mhps_low_R,mhps_low_i,mhps_high_g,mhps_high_R,mhps_high_i,\
	mhps_non_zero_g,mhps_non_zero_R,mhps_non_zero_i,mhps_pn_flag_g,mhps_pn_flag_R,mhps_pn_flag_i,drw_omega_g,drw_omega_R,drw_omega_i,\
	drw_tau_g,drw_tau_R,drw_tau_i,amplitude_g,amplitude_R,amplitude_i,anderson_darling_g,anderson_darling_R,anderson_darling_i,\
	autocorrelation_length_g,autocorrelation_length_R,autocorrelation_length_i,beyond1std_g,beyond1std_R,beyond1std_i,con_g,con_R,con_i,eta_e_g,\
	eta_e_R,eta_e_i,gskew_g,gskew_R,gskew_i,maxslope_g,maxslope_R,maxslope_i,meanmag_g,meanmag_R,meanmag_i,meanvariance_g,meanvariance_R,\
	meanvariance_i,medianabsdev_g,medianabsdev_R,medianabsdev_i,medianbrp_g,medianbrp_R,medianbrp_i,pairslopetrend_g,pairslopetrend_R,\
	pairslopetrend_i,percent_amplitude_g,\
	percent_amplitude_R,percent_amplitude_i,q31_g,q31_R,q31_i,rcs_g,rcs_R,rcs_i,skew_g,skew_R,skew_i,smallkurtosis_g,smallkurtosis_R,\
	smallkurtosis_i,stdmag_g,stdmag_R,stdmag_i,stetsonk_g,stetsonk_R,stetsonk_i,p_chi_g,p_chi_R,p_chi_i,ex_var_g,ex_var_R,ex_var_i,\
	iar_phi_g,iar_phi_R,iar_phi_i,regression_slope_g,regression_slope_R,regression_slope_i,delta_mag_fid_g,delta_mag_fid_R,delta_mag_fid_i,\
	harmonics_mag_1_g,harmonics_mag_1_R,harmonics_mag_1_i,harmonics_mag_2_g,harmonics_mag_2_R,harmonics_mag_2_i,harmonics_mag_3_g,\
	harmonics_mag_3_R,harmonics_mag_3_i,harmonics_mag_4_g,harmonics_mag_4_R,harmonics_mag_4_i,harmonics_mag_5_g,harmonics_mag_5_R,\
	harmonics_mag_5_i,harmonics_mag_6_g,harmonics_mag_6_R,harmonics_mag_6_i,harmonics_mag_7_g,harmonics_mag_7_R,harmonics_mag_7_i,\
	harmonics_phi_1_g,harmonics_phi_1_R,harmonics_phi_1_i,harmonics_phi_2_g,harmonics_phi_2_R,harmonics_phi_2_i,harmonics_phi_3_g,harmonics_phi_3_R,\
	harmonics_phi_3_i,harmonics_phi_4_g,harmonics_phi_4_R,harmonics_phi_4_i,harmonics_phi_5_g,harmonics_phi_5_R,harmonics_phi_5_i,\
	harmonics_phi_6_g,harmonics_phi_6_R,harmonics_phi_6_i,harmonics_phi_7_g,harmonics_phi_7_R,harmonics_phi_7_i,harmonics_mse_g,\
	harmonics_mse_R,harmonics_mse_i,harmonics_chi_per_degree_g,harmonics_chi_per_degree_R,harmonics_chi_per_degree_i\
	from featuretable where alert_id = '%s'"%(alert_id)
	
	#chunk_size = 500 # Number of rows per chunk
	

	# Use a for loop to iterate over the chunks
	#print(f"Starting to process data in chunks of {chunk_size} rows...")

	#for chunk_featuretable in pd.read_sql(sql_query, dbconn, chunksize=chunk_size):
	#		print(f"Processing a new chunk with {len(chunk_featuretable)} rows")


	print('classify A')
	chunk_featuretable = pd.read_sql(sql_query, dbconn)
	print(pd.read_sql(sql_query, dbconn))
	print('chunk_featuretable')
	print(chunk_featuretable)

	# Read the SQL query results into a pandas DataFrame
	# This function eliminates the need for a separate cursor object and fetchall() call
	#feature_table = pd.read_sql_query(sql_query, dbconn,chunksize=500)

	# Optional: Print the first few rows and type to verify
	#print(feature_table.head())
	#print(type(feature_table))

	#keep: locus_id as a key for new table 
	#remove: 'locus_id','ztf_object_id','locus_ra','locus_dec'

			
	alertinfo = chunk_featuretable[['locus_id','date_alert_mjd','alert_id']]
	
	
	chunk_featuretable = chunk_featuretable.drop(columns=['locus_id', 'date_alert_mjd','alert_id'])
			
	chunk_featuretable = chunk_featuretable.replace(['Inlier', 'Outlier', 'TBD'],[0, 1,np.nan])

	chunk_featuretable = chunk_featuretable.replace(['Pass', 'Fail'],[0, 1])
			
			#print(chunk_featuretable)

			# #Splitting the data into independent and dependent variables
	X = chunk_featuretable.values
				
			#print(X)
			
	scaler = StandardScaler()
			
	X = scaler.fit_transform(X)
			
	predicted_labels = trained_classifier.predict(X)
			
	p_class = trained_classifier.predict_proba(X)
	print('p_class')
	print(p_class)



	print('write classification to db')
	try:	
		c_2.execute("insert or ignore into classification(alert_id,p_cvnova,p_e, p_lpv, p_puls, p_periodic_other, p_quas, p_sn , p_yso) values (?, ?, ?, ?, ?, ?, ?, ?, ?)",(alertinfo['alert_id'][0],float(p_class[0][0]),float(p_class[0][1]),float(p_class[0][2]),float(p_class[0][3]),float(p_class[0][4]),float(p_class[0][5]),float(p_class[0][6]),float(p_class[0][7])))
		
	except:
		print('error when writing classification to db')

	print('classification written to db')
			
		
		


def calculate_features_one_lc(param):
	
	locus_id = param
	features = pd.DataFrame()
    
    # here goes the code processing something, i.e.: open the file, carry out computations on light curve data
    
    
    #### Access ANTARES database to query the light curve
		
		
		
	locus = get_by_id(locus_id)  
			##### calculate features
			
					
			#####print(locus.lightcurve)

	print('calculating features for ', locus_id)
	lightcurve = locus.lightcurve.dropna()
		
	mhps_ratio = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	mhps_low = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	mhps_high = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	mhps_non_zero = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	mhps_pn_flag = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	drw_omega = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	drw_tau = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	amplitude= {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	anderson_darling = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	autocorrelation_length = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	beyond1std = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	con = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	eta_e = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	gskew = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	maxslope = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	meanmag = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	meanvariance = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	medianabsdev = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	medianbrp = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	pairslopetrend = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	percent_amplitude = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	q31 = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	rcs = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	skew = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	smallkurtosis = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	stdmag = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	stetsonk = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	p_chi = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	ex_var = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	iar_phi = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	regression_slope = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	delta_mag_fid = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	g_r_max = np.NaN
	g_r_mean = np.NaN
	best_period = np.NaN
	significance = np.NaN
	best_period_band = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	power_rate = {0.5: np.NaN, 0.333: np.NaN, 0.25: np.NaN, 2.0: np.NaN, 3.0: np.NaN, 4.0: np.NaN}

	harmonics_mag_1 = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	harmonics_mag_2 = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	harmonics_mag_3 = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	harmonics_mag_4 = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	harmonics_mag_5 = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	harmonics_mag_6 = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	harmonics_mag_7 = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
				
			
	harmonics_phi_1 = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	harmonics_phi_2 = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	harmonics_phi_3 = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	harmonics_phi_4 = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	harmonics_phi_5 = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	harmonics_phi_6 = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	harmonics_phi_7 = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}

	harmonics_mse = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}
	harmonics_chi_per_degree = {'g': np.NaN, 'R': np.NaN, 'i': np.NaN}

	n_harmonics = 7
				
			
	g_r_max = np.NaN
	g_r_mean = np.NaN
	best_period = np.NaN
	significance = np.NaN
	
	writeflag = 0
				

			
	if(len(lightcurve) > 10):

				
				
	
				#lightcurve=lightcurve.drop(['time', 'alert_id', 'ant_survey', 'ant_ra', 'ant_dec', 'ant_mag',
				#'ant_magerr', 'ant_maglim', 'ant_magulim_corrected', 'ant_magllim_corrected'], axis=1)

				writeflag = 1

				columns_to_keep = ['ant_mjd', 'ant_passband', 'ant_mag_corrected', 'ant_magerr_corrected']	

				lightcurve = lightcurve[columns_to_keep]



				lightcurve = lightcurve.rename(columns={"ant_mjd": "time", "ant_passband": "band", "ant_mag_corrected": "magnitude", "ant_magerr_corrected": "error" })

				lightcurve['oid'] = locus_id
				lightcurve.set_index('oid', inplace=True)


				band = lightcurve['band'].values
				available_bands = np.unique(band)
				
				
			#g-r color obtained using the brightest lc_diff (difference light curve) magnitude in each band
				
				
				
				band_detections_g = lightcurve[lightcurve['band'] == 'g'].sort_values('time')

				
				band_detections_r = lightcurve[lightcurve['band'] == 'R'].sort_values('time')

				if (len(band_detections_g) > 0 and len(band_detections_r)>0):

					mag_g = band_detections_g['magnitude'].values.astype(np.double)

					mag_r = band_detections_r['magnitude'].values.astype(np.double)
					
					
					
					#print(mag_g)
					
					g_r_max = np.max(mag_g) - np.max(mag_r)
				
					#print(g_r_max)


					#print('g_r_mean')
				#g-r color obtained using the brightest lc_diff (difference light curve) magnitude in each band


					g_r_mean = np.mean(mag_g) - np.mean(mag_r)



			
			
				#print(' ')
				#print('P4J periods')
				my_per = P4J.MultiBandPeriodogram(method='MHAOV') # QMI based on Euclidean distance


				time = lightcurve['time'].values
				mag = lightcurve['magnitude'].values
				band = lightcurve['band'].values
				err = lightcurve['error'].values

				my_per.set_data(time, mag, err, band)
				my_per.frequency_grid_evaluation(fmin=0.0, fmax=5.0, fresolution=1e-3)  # frequency sweep parameters
				my_per.finetune_best_frequencies(fresolution=1e-4, n_local_optima=10)
				#freq, per is the complete periodogram
				freq, per = my_per.get_periodogram()
				fbest, pbest = my_per.get_best_frequencies() # Return best n_local_optima frequencies

								#best frequency
								
				best_freq = fbest[0]
				#print('fbest (best frequency)')
				#print(best_freq)
				best_period = 1.0/best_freq
				

				#			#best periodogram value
				#print('pbest (periodogram value of fbest)')
				#print(pbest[0])



							

				#false alarm prob
				period_candidate = 1.0/best_freq


				#print('available_bands: ', available_bands)
							#print('band')
							#print(band)

				period_candidates_per_band = []
				for band in available_bands:
										#print(b)
							if band not in available_bands:
								period_candidates_per_band.extend([np.nan])
								continue
							best_freq_band = my_per.get_best_frequency(band)
							#print('best_freq_band ', best_freq_band)
							# Getting best period
							p = 1.0 / best_freq_band

							# Calculating delta period
							delta_period_band = np.abs(period_candidate - p)
							period_candidates_per_band.extend([p])
								
							best_period_band[band]=p
							#print(band)
							#print(best_period_band)

				# Significance estimation
				entropy_best_n = 100
				top_values = np.sort(per)[-entropy_best_n:]
				normalized_top_values = top_values + 1e-2
				normalized_top_values = normalized_top_values / np.sum(normalized_top_values)
				entropy = (-normalized_top_values * np.log(normalized_top_values)).sum()
				significance = 1 - entropy / np.log(entropy_best_n)

								
				#print('period multiband: ', period_candidate)
				#print('significance: ', significance)
				
				


				#print('Power_rate')
			
				try:
					
					max_index = per.argmax()
					period = 1.0 / freq[max_index]
					period_power = per[max_index]

					if not is_sorted(freq):
							order = freq.argsort()
							freq = freq[order]
							per = per[order]
					
					
					for period_factor in (0.5, 0.333, 0.25, 2.0, 3.0, 4.0):

						desired_period = period * period_factor
						desired_frequency = 1.0 / desired_period
						i = np.searchsorted(freq, desired_frequency)

						if i == 0:
								i = 0
						elif desired_frequency > freq[-1]:
								i = len(freq) - 1
						else:
								left = freq[i-1]
								right = freq[i]
								mean = (left + right) / 2.0
								if desired_frequency > mean:
									i = i
								else:
									i = i-1
						power_rate[period_factor] = per[i] / period_power
						
						#print('period_factor ', period_factor)
						#print(power_rate)
					
				except:
					for period_factor in (0.5, 0.333, 0.25, 2.0, 3.0, 4.0):
						power_rate[period_factor] = np.NaN
				
				
				
				
				
				# for all bands
				for band in available_bands:
				
				
					band_detections = lightcurve[lightcurve['band'] == band].sort_values('time')
					#print(band_detections)
					mag = band_detections['magnitude'].values.astype(np.double)
					magerr = band_detections['error'].values.astype(np.double)
					time = band_detections['time'].values.astype(np.double)
					
					#print(band)
					#print(time)
					
					if(len(mag)>=5):
					
						#print('MHPS')			
						t1=100
						t2=10
						dt=3.0
						epsilon=1.0
						
						try:
							
							ratio, low, high, non_zero, pn_flag = mhps.statistics(mag,magerr,time,t1,t2)
							
							mhps_ratio[band]=ratio
							mhps_low[band]=low
							mhps_high[band]=high
							mhps_non_zero[band]=non_zero
							mhps_pn_flag[band]=pn_flag
						
						except:
						
							mhps_ratio[band]=np.NaN
							mhps_low[band]=np.NaN
							mhps_high[band]=np.NaN
							mhps_non_zero[band]=np.NaN
							mhps_pn_flag[band]=np.NaN
						#print(band)
						#print(mhps_ratio['R'])
						#print(ratio,low,high,non_zero,pn_flag)


			#############
					
							
						#print(' ')
						#print('DRW')
					
						#print(band)
						
						try:
					
							t = time - time.min()
							m = mag - mag.mean()
							sq_error = magerr ** 2
					
							kernel = terms.RealTerm(a=1.0, c=10.0)
							gp = celerite2.GaussianProcess(kernel, mean=0.0)
				
							initial_params = np.zeros((2,), dtype=float)
							sol = minimize(
							neg_log_like,
							initial_params,
							method="L-BFGS-B",
							args=(gp, t, m, sq_error))
				
							optimal_params = np.exp(sol.x)
					
							drw_omega[band]=optimal_params[0]
							drw_tau[band]=1.0 / optimal_params[1]
							
						except:
							drw_omega[band] = np.NaN
							drw_tau[band] = np.NaN

		#				print(out_data)
				#out = pd.Series(
				#    data=out_data, index=self.get_features_keys_with_band(band))
				#return out
			# 
			#         features = detections.apply(lambda det: aux_function(det, band))
			#         features.index.name = 'oid'
			#         return features
			# 
			#     @lru_cache(1)
			#     def get_features_keys_without_band(self) -> Tuple[str, ...]:
			#         feature_names = 'GP_DRW_sigma', 'GP_DRW_tau'
			#         return feature_names
				
				
				
				
				
				
				
				
				
			# 
			# 
			# 	#GP_DRW_sigma	Amplitude of the variability at short timescales (t << tau), from DRW modeling	g and r Graham+2017
			# 	#GP_DRW_tau	Relaxation time (tau) from DRW modeling 	g and r	Graham+2017
			# 


						#print('Amplitude')
				
				
					
			#  
			# 	
			# 	
			# 	#Amplitude	Half of the difference between the median of the maximum 5% and of the minimum 5% magnitudes	g and r	Richards+2011	feature_amplitude_magn_(g,r) is a bit different 
			# 
			# 
			# 	Amplitude_g
			# 	Amplitude_R
			# 	Amplitude_i

						try:
							
							sorted_mag = np.sort(mag)
							
							n = len(mag)

							amplitude[band] = (np.median(sorted_mag[int(-math.ceil(0.05 * n)):]) -
								np.median(sorted_mag[0:int(math.ceil(0.05 * n))])) / 2.0
						
							#print('AndersonDarling')

							ander = stats.anderson(mag)[0]
							anderson_darling[band] = 1 / (1.0 + np.exp(-10 * (ander - 0.3))) # between 0 and 1
						
						except:
							anderson_darling[band] = np.NaN

						#print('Autocor_length')
						
						try:
							nlags = 100

							ac = stattools.acf(mag, nlags=nlags, fft=False)

							autocor_length = next((index for index, value in
							enumerate(ac) if value < np.exp(-1)), None)

							while autocor_length is None:
								if nlags > len(mag):
									#warnings.warn('Setting autocorrelation length as light curve length')
									autocor_length = len(mag)
								else:
								
									nlags = nlags + 100
									ac = stattools.acf(mag, nlags=nlags, fft=False)
									autocor_length = next((index for index, value in
										enumerate(ac) if value < np.exp(-1)), None)     
									
							autocorrelation_length[band] = autocor_length
							
						except:
							autocorrelation_length[band] = np.NaN
				
					
						#print(band)
						#print(autocor_length)
				
				

						#print('Beyond1Std')

						
						try:
						
							n = len(mag)
							#print(n)
							weighted_mean = np.average(mag, weights=1 / magerr ** 2)
						
							# Standard deviation with respect to the weighted mean
						
							var = sum((mag - weighted_mean) ** 2)
							std = np.sqrt((1.0 / (n - 1)) * var)
						
							count = np.sum(np.logical_or(mag > weighted_mean + std,
									mag < weighted_mean - std))
							beyond1std[band] = float(count) / n
						
						except:
						
							beyond1std[band] = np.NaN
					
					
						#print(band)
						#print(beyond1std)
				
				

						#print('Con')
			#Index introduced for selection of variable starts from OGLE database.


			#To calculate Con, we counted the number of three consecutive measurements
			#  that are out of 2sigma range, and normalized by N-2
						try:
						
							
							consecutiveStar=3
						
							N = len(mag)
							if N < consecutiveStar:
								con= 0
							
							else:
							
								sigma = np.std(mag)
								m = np.mean(mag)
								count = 0

								for i in range(N - consecutiveStar + 1):
									flag = 0
									for j in range(consecutiveStar):
										if (mag[i + j] > m + 2 * sigma
											or mag[i + j] < m - 2 * sigma):
											flag = 1
										else:
											flag = 0
										break
									if flag:
										count = count + 1
								
								con[band] =  count * 1.0 / (N - consecutiveStar + 1)

						except:
						
							con[band] = np.NaN
						#print(band)
						#print(con)

						#print('Eta_e')
				


						try:
							
							w = 1.0 / np.power(np.subtract(time[1:], time[:-1]), 2)
							sigma2 = np.var(mag)

							S1 = np.sum(w * np.power(np.subtract(mag[1:], mag[:-1]), 2))
							S2 = np.sum(w)

							eta_e[band] = (1 / sigma2) * (S1 / S2)
							
						except:
							eta_e[band] = np.NaN
						
						
						#print(band)
						#print(eta_e)
					
					
						#print('Gskew')
						
						
						try:
					
							median_mag = np.median(mag)
							F_3_value, F_97_value = np.percentile(mag, (3, 97))

							gskew[band] = (np.median(mag[mag <= F_3_value]) +
							np.median(mag[mag >= F_97_value])
							- 2*median_mag)
						
							#print(band)
							#print(gskew)

							#print('MaxSlope')
							
							slope = np.abs(mag[1:] - mag[:-1]) / (time[1:] - time[:-1])
							slope = slope[np.isfinite(slope)]
							#print(slope)
							if(len(slope)>0):
								maxslope[band] = np.max(slope[np.isfinite(slope)]) # ignore any NaN and inf
						
						except:
							maxslope[band] = np.NaN
						
						
						#print(band)
						#print(maxslope)
					

						#print('Mean')
						
						try:
						
							meanmag[band] = np.mean(mag)
						except:
							meanmag[band] = np.NaN
					
						#print(band)
						#print(meanmag)


						#print('Meanvariance')
				
						try:
							meanvariance[band] = np.std(mag) / np.mean(mag)
						except:
							meanvariance[band] = np.NaN
					
						#print(band)
						#print(meanvariance)


						#print('MedianAbsDev')
						
						try:
						
							median = np.median(mag)

							devs = (abs(mag - median))

							medianabsdev[band] = np.median(devs)
							
						except:
							medianabsdev[band] = np.NaN

					
						#print(band)
						#print(medianabsdev)



						#print('MedianBRP')
				
						"""Median buffer range percentage
						Fraction (<= 1) of photometric points within amplitude/10 of the median magnitude"""
				
						try:
							median = np.median(mag)
							amp = (np.max(mag) - np.min(mag)) / 10
							n = len(mag)
						
							count = np.sum(np.logical_and(mag < median + amp,
									mag > median - amp))

							medianbrp[band] = float(count) / n
						
						except:
						
							medianbrp[band] = np.NaN
						#print(band)
						#print(medianbrp)
					
				
						#print('PairSlopeTrend')
					
						"""
						Considering the last 30 (time-sorted) measurements of source magnitude,
						the fraction of increasing first differences minus the fraction of
						decreasing first differences.
						"""
						
						try:
						
								
							data_last = mag[-30:]
						
							pairslopetrend[band] = (float(len(np.where(np.diff(data_last) > 0)[0]) -
							len(np.where(np.diff(data_last) <= 0)[0])) / 30)
						
						except:
							pairslopetrend[band] = np.NaN
						
						#print(band)
						#print(pairslopetrend)
						#print('PercentAmplitude')
						try:
						
							
							median_data = np.median(mag)
							distance_median = np.abs(mag - median_data)
							max_distance = np.max(distance_median)
						
							percent_amplitude[band] = max_distance / median_data
						
						except:
						
							percent_amplitude[band] = np.NaN
						#print(band)
						#print(percent_amplitude)
						
				
			


						#print('Q31')
						
						try:
						
							percentiles = np.percentile(mag, (25, 75))
							q31[band] = percentiles[1] - percentiles[0]
						except:
							q31[band] = np.NaN
			
						#print(band)
						#print(q31)
						
				
				

						#print('Rcs')
						
						try:
						
							sigma = np.std(mag)
							N = len(mag)
							m = np.mean(mag)
							s = np.cumsum(mag - m) * 1.0 / (N * sigma)
							rcs[band] = np.max(s) - np.min(s)
						
						except:
							rcs[band] = np.NaN
							
						#print(band)
						#print(rcs)


						#print('Skew')
						
						try:
						
							skew[band] = stats.skew(mag)
						except:
							skew[band] = np.NaN

						#print(band)
						#print(skew)

			
			




						#print('SmallKurtosis')
							
						"""Small sample kurtosis of the magnitudes.

						See http://www.xycoon.com/peakedness_small_sample_test_1.htm
						"""
						try:
							n = len(mag)
							mean = np.mean(mag)
							std = np.std(mag)
						
						
							S = sum(((mag - mean) / std) ** 4)
							c1 = float(n * (n + 1)) / ((n - 1) * (n - 2) * (n - 3))
							c2 = float(3 * (n - 1) ** 2) / ((n - 2) * (n - 3))
						
							smallkurtosis[band] =  c1 * S - c2
						except:
							smallkurtosis[band] = np.NaN
					
						#print(band)
						#print(smallkurtosis)

			
						#print('std')

						try:
							stdmag[band] = np.std(mag)
						except:
							stdmag[band] = np.NaN
							
						#print(band)
						#print(std)





						#print('StetsonK')
						
						try:
							
							n = len(mag)
						
							mean_mag = (np.sum(mag/(magerr*magerr))/np.sum(1.0 / (magerr * magerr)))
							sigmap = (np.sqrt(n * 1.0 / (n - 1)) * (mag - mean_mag) / magerr)
						
							stetsonk[band] = (1 / np.sqrt(n * 1.0) * np.sum(np.abs(sigmap)) / np.sqrt(np.sum(sigmap ** 2)))
						
						except:
							stetsonk[band] = np.NaN
					
						#print(band)
						#print(stetsonk)




						#print('Pvar')
							
						"""
						Calculate the probability of a light curve to be variable.
						"""	
						try:
							mean_mag = np.mean(mag)
						
							nepochs = float(len(mag))
						
							chi = np.sum((mag - mean_mag)**2. / magerr**2.)
							p_chi[band] = chi2.cdf(chi, (nepochs-1))
						except:
							p_chi[band] = np.NaN
						
							
						#print(band)
						#print(p_chi)


						#print('ExcessVar')
				
						"""
						Calculate the excess variance,which is a measure of the intrinsic variability amplitude.
						"""
						try:
						
							mean_mag = np.mean(mag)
							nepochs = float(len(mag))
						
							a = (mag-mean_mag)**2
							ex_var[band] = (np.sum(a-magerr**2) / (nepochs * (mean_mag ** 2)))

						except:
							ex_var[band] = np.NaN
							
						#print(band)
						#print(ex_var)




						#print('IAR_phi')
				

						"""
						Functions to compute an IAR model with Kalman filter.
						Author: Felipe Elorrieta.
						"""
						try:
							
							if np.sum(magerr) == 0:
								err = np.zeros(len(mag))
							else:
								err = magerr
							
							std = np.std(mag, ddof=1)
							ynorm = (mag-np.mean(mag)) / std
							deltanorm = err / std
						
						
							out = minimize_scalar(IAR_phi_kalman, args=(time, ynorm, deltanorm), bounds=(0, 1), method="bounded",
							options={'xatol': 1e-12, 'maxiter': 50000})
						
							phi = out.x
							try:
								phi = phi[0][0]
								
							except:
								phi = phi
								
							iar_phi[band]=phi 
							
							
						except:
							iar_phi[band] = np.NaN
						
						#print(band)
						#print(iar_phi)
					

						#print('LinearTrend')
						#print(time)
						#print(mag)
						try:
							regression_slope[band] = stats.linregress(time, mag)[0]
						except:
							regression_slope[band] = np.NaN
					
					
						#print(band)
						#print(regression_slope)





						#print('delta_mag_fid')
				# Difference between maximum and minimum observed magnitude in a given band	g and r
						try:
						
							delta_mag_fid[band] = np.max(mag) - np.min(mag)
						except:
							delta_mag_fid[band] = np.NaN
							
	
	
						try:
							
							error = magerr + 10 ** -2
								
							omega = [np.array([[1.] * len(time)])]
							timefreq = (2.0 * np.pi * best_freq * np.arange(1, n_harmonics + 1)).reshape(1, -1).T * time
							omega.append(np.cos(timefreq))
							omega.append(np.sin(timefreq))
							omega = np.concatenate(omega, axis=0).T
								
							inverr = 1.0 / error
							# weighted regularized linear regression
								
							w_a = inverr.reshape(-1, 1) * omega
							w_b = (mag * inverr).reshape(-1, 1)
							coeffs = np.matmul(np.linalg.pinv(w_a), w_b).flatten()
							fitted_magnitude = np.dot(omega, coeffs)
							coef_cos = coeffs[1:n_harmonics + 1]
							coef_sin = coeffs[n_harmonics + 1:]
							coef_mag = np.sqrt(coef_cos ** 2 + coef_sin ** 2)
							coef_phi = np.arctan2(coef_sin, coef_cos)

							# Relative phase
							coef_phi = coef_phi - coef_phi[0] * np.arange(1, n_harmonics + 1)
							coef_phi = coef_phi[1:] % (2 * np.pi)
								
							mse = np.mean((fitted_magnitude - mag) ** 2)
								
							# Calculate reduced chi-squared statistic
							chi = np.sum((fitted_magnitude - mag) ** 2 / (error + 5) ** 2)
							chi_den = len(fitted_magnitude) - (1 + 2*n_harmonics)
							if chi_den >= 1:
								chi_per_degree = chi / chi_den
							else:
								chi_per_degree = np.nan
								
							out = pd.Series(  data=np.concatenate([coef_mag, coef_phi, np.array([mse, chi_per_degree])]))
							#print(out)
							
							#print(band)
							
							#print(coef_mag)
							#print(coef_phi)
							#print(mse)
							#print(chi_per_degree)
							
							harmonics_mag_1[band] = coef_mag[0]
							harmonics_mag_2[band] = coef_mag[1]
							harmonics_mag_3[band] = coef_mag[2]
							harmonics_mag_4[band] = coef_mag[3]
							harmonics_mag_4[band] = coef_mag[4]
							harmonics_mag_6[band] = coef_mag[5]
							harmonics_mag_7[band] = coef_mag[6]
											
							harmonics_phi_1[band] = coef_phi[0]
							harmonics_phi_2[band] = coef_phi[1]
							harmonics_phi_3[band] = coef_phi[2]
							harmonics_phi_4[band] = coef_phi[3]
							harmonics_phi_5[band] = coef_phi[4]
							harmonics_phi_6[band] = coef_phi[5]
							harmonics_phi_7[band] = coef_phi[6]
							
							harmonics_mse[band] = mse 
							harmonics_chi_per_degree[band] = chi_per_degree 
				
						except:
						
						
							harmonics_mag_1[band] = np.NaN
							harmonics_mag_2[band] = np.NaN
							harmonics_mag_3[band] = np.NaN
							harmonics_mag_4[band] = np.NaN
							harmonics_mag_4[band] = np.NaN
							harmonics_mag_6[band] = np.NaN
							harmonics_mag_7[band] = np.NaN
											
							harmonics_phi_1[band] = np.NaN
							harmonics_phi_2[band] = np.NaN
							harmonics_phi_3[band] = np.NaN
							harmonics_phi_4[band] = np.NaN
							harmonics_phi_5[band] = np.NaN
							harmonics_phi_6[band] = np.NaN
							harmonics_phi_7[band] = np.NaN
							
							harmonics_mse[band] = np.NaN 
							harmonics_chi_per_degree[band] = np.NaN
		
		
	#############	
		
	features = [g_r_max, g_r_mean, best_period, significance,
						best_period_band['g'], best_period_band['R'],
						best_period_band['i'], power_rate[0.5], power_rate[0.333], 
						power_rate[0.25], power_rate[2], power_rate[3],
						power_rate[4],mhps_ratio['g'], mhps_ratio['R'], mhps_ratio['i'],	mhps_low['g'], mhps_low['R'], mhps_low['i'], 
						mhps_high['g'], mhps_high['R'], mhps_high['i'], mhps_non_zero['g'], 
						mhps_non_zero['R'], mhps_non_zero['i'], mhps_pn_flag['g'], mhps_pn_flag['R'], \
						mhps_pn_flag['i'], drw_omega['g'], drw_omega['R'], \
						drw_omega['i'], drw_tau['g'], drw_tau['R'], drw_tau['i'], amplitude['g'], amplitude['R'],\
						amplitude['i'], anderson_darling['g'], anderson_darling['R'], anderson_darling['i'], autocorrelation_length['g'], 
						autocorrelation_length['R'], autocorrelation_length['i'], beyond1std['g'], \
						beyond1std['R'], beyond1std['i'], con['g'],
						con['R'], con['i'], eta_e['g'], eta_e['R'], eta_e['i'], gskew['g'],\
						gskew['R'], gskew['i'], maxslope['g'], maxslope['R'], \
						maxslope['i'], meanmag['g'], meanmag['R'], meanmag['i'], meanvariance['g'], meanvariance['R'], 
						meanvariance['i'], medianabsdev['g'], medianabsdev['R'], medianabsdev['i'], 
						medianbrp['g'], medianbrp['R'], medianbrp['i'], pairslopetrend['g'], pairslopetrend['R'], pairslopetrend['i'],
						percent_amplitude['g'], percent_amplitude['R'], percent_amplitude['i'], 
						q31['g'] , q31['R'], 
						q31['i'], rcs['g'], rcs['R'], rcs['i'], skew['g'], skew['R'], skew['i'],
						smallkurtosis['g'], smallkurtosis['R'], 
						smallkurtosis['i'], stdmag['g'], stdmag['R'], stdmag['i'], stetsonk['g'], 
						stetsonk['R'], stetsonk['i'], p_chi['g'], p_chi['R'], p_chi['i'], ex_var['g'], 
						ex_var['R'], ex_var['i'], iar_phi['g'], 
						iar_phi['R'], iar_phi['i'], regression_slope['g'], regression_slope['R'], 
						regression_slope['i'], delta_mag_fid['g'], delta_mag_fid['R'], delta_mag_fid['i'], harmonics_mag_1['g'], 
						harmonics_mag_1['R'], harmonics_mag_1['i'], harmonics_mag_2['g'], harmonics_mag_2['R'], 
						harmonics_mag_2['i'], harmonics_mag_3['g'], harmonics_mag_3['R'], harmonics_mag_3['i'], 
						harmonics_mag_4['g'], harmonics_mag_4['R'], harmonics_mag_4['i'],
						harmonics_mag_5['g'], harmonics_mag_5['R'], harmonics_mag_5['i'], harmonics_mag_6['g'], 
						harmonics_mag_6['R'], harmonics_mag_6['i'], harmonics_mag_7['g'], 
						harmonics_mag_7['R'], harmonics_mag_7['i'], harmonics_phi_1['g'], 
						harmonics_phi_1['R'], harmonics_phi_1['i'], harmonics_phi_2['g'], 
						harmonics_phi_2['R'], harmonics_phi_2['i'], harmonics_phi_3['g'], 
						harmonics_phi_3['R'], harmonics_phi_3['i'], 
						harmonics_phi_4['g'], harmonics_phi_4['R'], 
						harmonics_phi_4['i'], harmonics_phi_5['g'], harmonics_phi_5['R'], 
						harmonics_phi_5['i'], harmonics_phi_6['g'], harmonics_phi_6['R'], harmonics_phi_6['i'], 
						harmonics_phi_7['g'], harmonics_phi_7['R'], harmonics_phi_7['i'], harmonics_mse['g'], 
						harmonics_mse['R'], harmonics_mse['i'], harmonics_chi_per_degree['g'], harmonics_chi_per_degree['R'], 
						harmonics_chi_per_degree['i'], locus_id,writeflag]
	
	
	
				
	return features


	
def main():
	
	
	
	bands = ['g','R','i']
	
	
	PS1_final_RRLyr_candidates_RRab = np.genfromtxt('catalogs/PS1_final_RRLyr_candidates.csv', \
	names = 'ra,dec,objid',
	usecols = (0,1,8), \
	dtype = 'f8, f8, |U20', skip_header=1, delimiter=',')

	PS1_RRL_catalog = SkyCoord(ra=PS1_final_RRLyr_candidates_RRab['ra']*u.degree, dec=PS1_final_RRLyr_candidates_RRab['dec']*u.degree)
	print('PS1 loaded')
	
	
	"""
	Use this script as a starting point for streaming alerts from ANTARES.
	"""
	keyFile = open('keys.txt', 'r')
	API_KEY = keyFile.readline().rstrip()
	API_SECRET = keyFile.readline().rstrip()

	#consumer_secret = keyFile.readline().rstrip()
	#API_SECRET = keyFile.readline().rstrip()
	keyFile.close()


	print(API_KEY)
	print(API_SECRET)

	#TOPICS = ["extragalactic_staging", "nuclear_transient_staging"]
	#CONFIG = {
	#"api_key": API_KEY,
	#"api_secret": API_SECRET,
	#}



	client = StreamingClient(
		topics=["high_amplitude_variable_star_candidate_staging"],
		api_key=API_KEY,
		api_secret=API_SECRET,
		group="HernitschekNi"
		)
	

	#The poll method can be used to retrieve an alert. It returns a (topic, locus) tuple where topic is a string 
	#(in this example either "extragalactic_staging" or "nuclear_transient_staging") and locus is a Locus instance 
	#that contains the history of observations at the alert site. By default, this method will block indefinitely,
	#waiting for an alert. If you pass an argument to the timeout keyword, the method will return (None, None) 
	#after timeout seconds have elapsed:

	topic = 'high_amplitude_variable_star_candidate_staging'
	datetimenow = datetime.now()
	print(datetimenow)


## TODO: Add logging
	# disables logging from GPy.py unless they are at least WARNING

		#logging.getLogger("GP").setLevel(logging.WARNING)

		#logging.basicConfig(filename='logfiles/%s_%s__stream.log' % (datetimenow,topic),format='%(asctime)s %(message)s',
						#level=logging.INFO)
		#logging.info('start processing alert archive')


	f = open('logfiles/%s__stream.log' % (topic), "a+")





	current_dirs_parent = os.path.dirname(os.getcwd())
	db_path = current_dirs_parent + '/_broker_db/ztf_alerts_stream.db'
		
	dbconn = sqlite3.connect(db_path, isolation_level=None)	
	
	
	
	dbconn.execute('pragma journal_mode=wal;')

	c = dbconn.cursor()
	
	c_2 = dbconn.cursor()	
	
	c_3 = dbconn.cursor()

	#### make a loop and keep it running
	while True:
	
		print('try connecting ', datetime.now())
		f.write('try connecting ' + str( datetime.now()))
		print ( client.iter())
		
		try:
				for topic, locus in client.iter():
					
					print('topic ', topic)
					print('locus ', locus)
					logdate = datetime.now()

					print("{} received {} on {}".format(logdate,locus, topic))
					locus_id = locus.locus_id
					print('locus_id: ', locus_id)
					jdate = locus.alerts[-1].mjd+2400000.5
						
					t = Time(jdate, format='jd')
					
					alert_id = locus.alerts[-1].alert_id
							
					
				
					j=np.abs(locus.lightcurve.ant_mjd-locus.alerts[-1].mjd).argmin()

					
					ant_mag_corrected=locus.lightcurve.ant_mag_corrected[j]
					ant_passband=locus.lightcurve.ant_passband[j]
					
					
					print("date: {} {}".format(locus.alerts[-1].mjd, t.isot))
						# MJD field is the time of observation, which is in UTC
						
				
					#logging.info("timestamp %s, locus_id %s " % (datetime.datetime.now(),locus_id))
				
					#f.write("timestamp %s, locus_id %s\n" % (datetime.datetime.now(),locus_id))
					
					
					f.write("timestamp %s, locus_id %s, locus.alerts.alert_id %s\n" % (logdate,locus_id,locus.alerts[-1].alert_id))
					f.flush()
						
					
				
				
					# lightcurve:
					#print('list(locus.lightcurve):')
					#print(list(locus.lightcurve))
					
					#print("locus.lightcurve")
					#print(locus.lightcurve)
					
					#print("locus.lightcurve.time")
					#print(locus.lightcurve.time)
					
					#print("locus.lightcurve.ant_mjd")
					#print(locus.lightcurve.ant_mjd)
					#print("locus.lightcurve.ant_mag_corrected")
					#print(locus.lightcurve.ant_mag_corrected)
					
					
					#print(locus.properties)
					
					
					#print("feature_amplitude_magn_r")
					#print(locus.properties.get("feature_amplitude_magn_r"))
					
					##### STORE THIS:
					#ime stamp, alert name, locus name, date of alert, locus.properties
					
					
					

		#The magpsf property of an alert sent by ZTF is what is measured from the difference image. In Antares, we compute an 
		#add-on property called  ant_mag_corrected  that tries to account for the subtracted flux. So, when using Antares, 
		#if your application involves variable stars you should ideally use ant_mag_corrected.
		#Updating the documentation to clarify this confusion is in the team's to-do list.


		#locus properties (features):
		#Those particular properties are updated by a filter named IF_anomaly_detection.  
		#It runs every night. There's not a history for them on the locus.  



					
					print('calculate additional features')
					features = calculate_features_one_lc(locus_id)
		#if value does not exist in properties: write NaN 
					print(features)

					print('attempt write DB')
					print(alert_id)
					
					### write external classification information to database
					c.execute("insert or ignore into featuretable values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",(
					logdate, alert_id, locus_id, locus.ra, locus.dec, locus.alerts[-1].mjd, locus.properties.get("ztf_object_id"),
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
					ant_mag_corrected, ant_passband,features[0], features[1], features[2], features[3],
					features[4], features[5],features[6], features[7], features[8], features[9], features[10], features[11], 
					features[12], features[13], features[14], features[15], features[16], features[17], features[18], 
					features[19], features[20], features[21], features[22], features[23], features[24], features[25],
					features[26], features[27], features[28], features[29], features[30], features[31], features[32], features[33], 
					features[34], features[35], features[36], features[37], features[38], features[39], features[40], 
					features[41], features[42], features[43], features[44], features[45], features[46], features[47], features[48], 
					features[49], features[50], features[51],	features[52], features[53], features[54], features[55],
					features[56], features[57], features[58], features[59], features[60], features[61], features[62],
					features[63], features[64], features[65], features[66], features[67], features[68], features[69], features[70], features[71],
					features[72], features[73], features[74], features[75], features[76], features[77], features[78],
					features[79], features[80], features[81], features[82], features[83], features[84], features[85],
					features[86], features[87], features[88], features[89], features[90], features[91], features[92], features[93],
					features[94], features[95], features[96], features[97], features[98], features[99], 
					features[100], features[101], features[102], features[103], features[104], features[105], features[106], 
					features[107], features[108], features[109], features[110], features[111], features[112], features[113], 
					features[114], features[115], features[116], features[117], features[118], features[119],
					features[120], features[121], features[122], features[123], 
					features[124], features[125], features[126], features[127], features[128], features[129], 
					features[130], features[131], features[132], features[133], features[134], features[135],
					features[136], features[137], features[138], features[139], features[140], features[141], features[142], 
					features[143], features[144], features[145], features[146],
					features[147], features[148], features[149], features[150], 
					features[151], features[152], features[153], features[154], features[155],  features[156]) )
									
									
					dbconn.commit()							
					print('done write alert to DB')	
					
					
					
						
					# classify
					print('classify')
					classify(alert_id)
					
						
				
					# cross-match
					
					
					
					# check if already cross-matched
					
					c_3.execute('SELECT count(*) from crossmatches where locus_id = ?', (locus_id,))
					
					
					already_crossmatched=c_3.fetchone()[0]
					print('already_crossmatched: ', already_crossmatched)
					
					if(already_crossmatched==0):
					
						
						#if not cross-matched:

						matches = locus.catalog_objects

					
						for catalog_match in matches:
							
							print(catalog_match)    #this fills column "Catalog"
					
							match catalog_match:
								
								case "allwise":
									object_match = matches['allwise'][0]['designation']
									ra_match = matches['allwise'][0]['ra']
									dec_match = matches['allwise'][0]['decl']  
									
								case "csdr2":        
									object_match = matches['csdr2'][0]['name']
									ra_match = matches['csdr2'][0]['ra']
									dec_match = matches['csdr2'][0]['decl']
									
								case "vsx":        
									object_match = matches['vsx'][0]['name']
									ra_match = matches['vsx'][0]['raj2000']
									dec_match = matches['vsx'][0]['dej2000']  
									
								case "gaia_dr3_variability":        
									object_match = matches['gaia_dr3_variability'][0]['source']
									ra_match = matches['gaia_dr3_variability'][0]['ra_icrs']
									dec_match = matches['gaia_dr3_variability'][0]['de_icrs']  
									
								case "gaia_dr3_gaia_source":        
									object_match = matches['gaia_dr3_gaia_source'][0]['designation']
									ra_match = matches['gaia_dr3_gaia_source'][0]['ra']
									dec_match = matches['gaia_dr3_gaia_source'][0]['dec']           
								
								case "bright_guide_star_cat":   
									object_match = matches['bright_guide_star_cat'][0]['hstID']
									ra_match = matches['bright_guide_star_cat'][0]['RightAsc_deg']
									dec_match = matches['bright_guide_star_cat'][0]['Declination_deg']           
									
								case "asassn_variable_catalog_v2_20190802":        
									object_match = matches['asassn_variable_catalog_v2_20190802'][0]['asassn_name']
									ra_match = matches['asassn_variable_catalog_v2_20190802'][0]['raj2000']
									dec_match = matches['asassn_variable_catalog_v2_20190802'][0]['dej2000']                 
									
								case "2mass_psc":        
									object_match = matches['2mass_psc'][0]['designation']
									ra_match = matches['2mass_psc'][0]['ra']
									dec_match = matches['2mass_psc'][0]['decl']   
									
								case "linear_ll":        
									object_match = matches['linear_ll'][0]['linear']
									ra_match = matches['linear_ll'][0]['raj2000']
									dec_match = matches['linear_ll'][0]['dej2000']   
									
								case "sdss_stars":        
									object_match = matches['sdss_stars'][0]['Objid']
									ra_match = matches['sdss_stars'][0]['ra']
									dec_match = matches['sdss_stars'][0]['dec_']   
									
								case "gaia_edr3_distances_bailer_jones":        
									object_match = matches['gaia_edr3_distances_bailer_jones'][0]['source']
									ra_match = matches['gaia_edr3_distances_bailer_jones'][0]['ra_icrs']
									dec_match = matches['gaia_edr3_distances_bailer_jones'][0]['de_icrs']   
									
								case "sdss_gals":        
									object_match = matches['sdss_gals'][0]['Objid']
									ra_match = matches['sdss_gals'][0]['ra']
									dec_match = matches['sdss_gals'][0]['dec_']   
									
								case "milliquas":        
									object_match = matches['milliquas'][0]['rname']
									ra_match = matches['milliquas'][0]['ra']
									dec_match = matches['milliquas'][0]['dec']     
									
								case "tns_public_objects":        
									object_match = matches['tns_public_objects'][0]['name']
									ra_match = matches['tns_public_objects'][0]['ra']
									dec_match = matches['tns_public_objects'][0]['declination']               
									
								case "sdss_dr7":        
									object_match = matches['sdss_dr7'][0]['objid']
									ra_match = matches['sdss_dr7'][0]['raj2000']
									dec_match = matches['sdss_dr7'][0]['dej2000']               
									
								case "galex":        
									object_match = matches['galex'][0]['OBJID']
									ra_match = matches['galex'][0]['AVASPRA']
									dec_match = matches['galex'][0]['AVASPDEC']
									
								case "ned":        
									object_match = matches['ned'][0]['Object_Name']
									ra_match = matches['ned'][0]['RA_deg']
									dec_match = matches['ned'][0]['DEC_deg']     
									
								case "veron_agn_qso":        
									object_match = matches['veron_agn_qso'][0]['Name']
									ra_match = matches['veron_agn_qso'][0]['viz_RAJ2000']
									dec_match = matches['veron_agn_qso'][0]['viz_DEJ2000']           
									
								case "nyu_valueadded_gals":        
									object_match = matches['nyu_valueadded_gals'][0]['IND']
									ra_match = matches['nyu_valueadded_gals'][0]['RA']
									dec_match = matches['nyu_valueadded_gals'][0]['DEC']               
									
								case "RC3":        
									object_match = matches['RC3'][0]['name']
									ra_match = matches['RC3'][0]['ra']
									dec_match = matches['RC3'][0]['dec']                     
									
								case "veron_agn_qso":        
									object_match = matches['veron_agn_qso'][0]['Name']
									ra_match = matches['veron_agn_qso'][0]['viz_RAJ2000']
									dec_match = matches['veron_agn_qso'][0]['viz_DEJ2000']               
									
								case "xmm3_dr8":        
									object_match = matches['xmm3_dr8'][0]['iauname']
									ra_match = matches['xmm3_dr8'][0]['ra']
									dec_match = matches['xmm3_dr8'][0]['dec']               
									
								case "chandra_master_sources":        
									object_match = matches['chandra_master_sources'][0]['name']
									ra_match = matches['chandra_master_sources'][0]['ra']
									dec_match = matches['chandra_master_sources'][0]['dec']     
									
								case "vii_274_bzcat5":        
									object_match = matches['vii_274_bzcat5'][0]['name']
									ra_match = matches['vii_274_bzcat5'][0]['raj2000']
									dec_match = matches['vii_274_bzcat5'][0]['dej2000']     
									
								case _:	# matched to a catalog not in this list
									object_match = ''
									ra_match = np.nan
									dec_match = np.nan
									
							#print('object_match ', object_match)
							#print('ra_match ', ra_match)
							#print('dec_match ', dec_match)

							coord1 = SkyCoord(ra=locus.ra*u.deg, dec=locus.dec*u.deg, frame='icrs') # this is from the object in the broker
							#print('coord1')
							#print(coord1)

							coord2 = SkyCoord(ra=ra_match*u.deg, dec=dec_match*u.deg, frame='icrs') # this is from the matched one

							#print('coord2')
							#print(coord2)

							separation_match = (coord1.separation(coord2)).arcsecond
							
							print(separation_match)
							
							#print(locus_id)
							#print(catalog_match)
							
								
							c_2.execute("insert into crossmatches(locus_id, catalog, object, ra_cat, dec_cat, separation) values (?, ?, ?, ?, ?, ?)",(locus_id, catalog_match, object_match, ra_match, dec_match, separation_match) )
								
							dbconn.commit()
								
								
						# cross-match with PS1_RRL_catalog
						
						coord1 = SkyCoord(ra=locus.ra*u.degree, dec=locus.dec*u.degree)
						
						idx, d2d, d3d = coord1.match_to_catalog_sky(PS1_RRL_catalog)
						
						

						## keep if id d2d < 1.5 arcsec
						if(d2d < 1.5*u.arcsec):
							#print('PS1_RRL_catalog match')
							##print (idx,d2d,d3d)
							#print (PS1_final_RRLyr_candidates_RRab[idx])
							
							ps1_ra = PS1_final_RRLyr_candidates_RRab[idx][0]
							ps1_dec = PS1_final_RRLyr_candidates_RRab[idx][1]
							ps1_objid = PS1_final_RRLyr_candidates_RRab[idx][2]
							#print(ps1_ra,ps1_dec,ps1_objid)
							# insert the cross-match into table if no PS1 cross-match exists for this object
							#table is crossmatches with columns locus_id, ps1_ra, ps1_dec, ps1_objid
							
							#print('attempt write DB')
							#### write external classification information to database
							coord2 = SkyCoord(ra=ps1_ra*u.deg, dec=ps1_dec*u.deg, frame='icrs') # this is from the matched one

							separation_match = coord1.separation(coord2)
							c_2.execute("insert into crossmatches(locus_id, catalog, object, ra_cat, dec_cat, separation) values (?, ?, ?, ?, ?, ?)",(locus_id, 'PS1_RRL', ps1_objid, ps1_ra, ps1_dec, separation_match) )
							
							
							dbconn.commit()
							
						print('done write cross-matches to DB')	

					
					
					
					
					
					print('------')	

		except:
			print('an exception has occurred when processing alert')
			pass

					
		#			things I want to store:
		# alert name, locus name, current features from the locus
		# features are: 
		#print(locus.properties)

		# ---> first print, then check writing to my DB


		#things I want from their DB:
		#- RA, Dec
		#- cross-matches
		#- light curve: check how long it does go back, do I need to store it for complete lc?
		# ---> check acessing from their DB, then check whether I get as many as their  num_alerts



		#next things:
		#- I store which alerts received in a DB
		#- I store calculated features to get a history of features
		#--> then change the frontend website
		#- cross-matches: when clicked, show all cross-matches, and then you can fold down each to see details (access directly from db)
		#- I calc my own features
		#- lc if requested is loaded from Antares to calculate my own features (I don't store the light curve)
		#- Antares properties if requested are loaded from Antares
		#- plots are done from Antares db
		#- crossmatches: show whch cross-matches, and then when clicking down show the details with mag, maybe image
		#- add in information for follow-up, like from the caltech software


		#works like that:
		#- gets alerts
		#- stores them (without lc)
		#- if features change: retrieve lc from database



					#lightcurve:
					#['time', 'alert_id', 'ant_mjd', 'ant_survey', 'ant_ra', 'ant_dec', 'ant_passband', 'ant_mag', 'ant_magerr', 'ant_maglim', 'ant_mag_corrected', 'ant_magerr_corrected', 'ant_magulim_corrected', 'ant_magllim_corrected']

					
					# this is the time series - works:
					#print('timeseries:')
					#print(list(locus.timeseries))
					#print(locus.timeseries)
					
					# these are the features - works:
					# features
					#print('features:')
					#print(locus.properties)
					#https://antares.noirlab.edu/properties
					#ant_* are normalized
					
					# these are the cross-matches - works:
					# cross-matches
					#catalog_objects (Optional[List[dict]])
					#A list of catalog objects that are associated with this locus. 
					#If None, they will be loaded on first access from the ANTARES HTTP API.
					
					#print('cross-matches:')
					#print(locus.catalog_objects)
					
					# plot - works:
					#plot_lightcurve(locus_id,locus.lightcurve,'testplots')
					
					
					

			#locus_id (str) – ANTARES ID for this object.

			#ra (float) – Right ascension of the centroid of alert history.

			#dec (float) – Declination of the centroid of alert history.

			#properties (dict) – A dictionary of ANTARES- and user-generated properties that are updated every time there is activity on this locus (e.g. a new alert).

			#tags (List[str]) – A list of strings that are added to this locus by ANTARES- and user-submitted filters that run against the real-time alert stream.

			#alerts (Optional[List[Alert]]) – A list of alerts that are associated with this locus. If None, the alerts will be loaded on first access from the ANTARES HTTP API.

			#catalogs (Optional[List[str]]) – Names of catalogs that this locus has been associated with.

			#catalog_objects (Optional[List[dict]]) – A list of catalog objects that are associated with this locus. If None, they will be loaded on first access from the ANTARES HTTP API.

			#lightcurve (Optional[pd.DataFrame]) – Data frame representation of a subset of normalized alert properties. If None it will be loaded on first access from the ANTARES HTTP API.

			#watch_list_ids (Optional[List[str]]) – A list of IDs corresponding to user-submitted regional watch lists.

			#watch_object_ids (Optional[List[str]]) – A list of IDs corresponding to user-submitted regional watch list objects.

			
					
					
					
					
					
					#
		#When an alert exits the pipeline it has been flagged with catalog matches, 
		#arbitrary new data properties generated by the filters, and stream associations. 
					
			

			
			#except:
			#	print("no alerts")
			#	pass
			
			
			#except Exception as error:
		# handle the exception
				#print("An exception occurred:", error) 
				#pass
				
	#finally:
				#logging.info('finished processing alert archive')
				
				
	f.close()
	client.close()
		
	
	dbconn.close()


	#topic, locus = client.poll(timeout=10)
	#if locus:
		#print("received locus {} on topic {}".format(locus, topic))
		
		


				
		#print(locus.alerts)
		#print(locus.lightcurve)
		
		
		#alert_id = locus.alerts[0].alert_id
		
		#print('alert_id: ', alert_id)
		
		
		#for i in range(0,len(locus.alerts)):
		#	print(locus.alerts[i].mjd)
				
		
		#this is the same to download without the ANTARES client:
		# I can use this to maybe pull the lightcurve later
		#lc = requests.get('https://api.antares.noirlab.edu/v1/loci/%s'%(locus.locus_id)).json()['data']['attributes']['lightcurve']
		#ts = TimeSeries.read(lc, format='ascii.csv')
		#print(ts)
		
		


		# this is okay:	
		
		#print(locus.lightcurve)

		
		#print(locus.alerts[-1].mjd)  # this is the current alert that came in
		
		#for i in range(0,len(locus.alerts)):
		#	print(locus.alerts[i].mjd)
	 
		
	#else:
		#print("waited 10 seconds but didn't get an alert")

############

if __name__ == "__main__":
    main()  
  
  
  
  
	




##########



#topics:
	
#extragalactic_staging
#high_amplitude_transient_candidate
#high_amplitude_variable_star_candidate_staging
#high_snr_staging
#in_m31_staging
#iso_forest_anomaly_detection
#nova_test
#nuclear_transient_staging
#refitt_newsources_snrcut_staging
#sso_candidates_staging
#sso_confirmed_staging
#young_extragalactic_candidate_staging


#The time format is modified julian date (MJD).  Which is the julian date minus 2400000.5



#### production version below - freeze 30 Oct, 2020
#from statsmodels.stats.weightstats import DescrStatsW
#import numpy as np
#from astropy.table import MaskedColumn

#class HighAmp_v2(dk.Filter):
    #ERROR_SLACK_CHANNEL = None
    
    #INPUT_LOCUS_PROPERTIES = [
        #'ztf_object_id',
    #]

    ### bare absolute minimum alert properties proving it's good to go
    #INPUT_ALERT_PROPERTIES = [
        #'ztf_fid',
        #'ztf_magpsf',
        #'ztf_sigmapsf',
        #'ant_mjd',
        #'ant_survey'
    #]    
    
    
    #OUTPUT_TAGS = [
        #{
            #'name': 'high_amplitude_transient_candidate',
            #'description': 'Locus - a transient candidate - exhibits a high amplitude',
        #},
        #{
            #'name': 'high_amplitude_variable_star_candidate',
            #'description': 'Locus - a variable star candidate - exhibits a high amplitude',
        #},
    #]
    
    #def _is_var_star(self, df, match_radius_arcsec=1.5, star_galaxy_threshold=0.4):
        #"""
        #Returns a boolean indicating if the locus is a variable star.

        #Parameters
        #----------
        #df : Astropy TimeSeries
            #QTable with the properties of all the alerts/detections for the locus
        #match_radius_arcsec : float, default 1.5
            #Upper bound of matching radius (in arcsec) used to find the counterpart of the locus in PS1 and in the ZTF template    
        #star_galaxy_threshold : float, 0.4
            #Lower bound of star-galaxy score of the PS1 counterpart. Value closer to 1 indicates a star. 

        #"""

        ### NOTE: there are cases where there may be a ps1 nn but not on ZTF (duh, ps1 is deeper) so also using distnr
        
        #if isinstance(df['ztf_distnr'], MaskedColumn) == True:
            #distnr = df['ztf_distnr'].filled(fill_value=np.nan).data
        #else:
            #distnr = df['ztf_distnr'].data
            
        #if isinstance(df['ztf_distpsnr1'], MaskedColumn) == True:
            #distpsnr1 = df['ztf_distpsnr1'].filled(fill_value=np.nan).data
        #else:
            #distpsnr1 = df['ztf_distpsnr1'].data

        #if isinstance(df['ztf_sgscore1'], MaskedColumn) == True:
            #sgscore = df['ztf_sgscore1'].filled(fill_value=np.nan).data
        #else:
            #sgscore = df['ztf_sgscore1'].data
            
        #return np.median(distpsnr1[np.isfinite(distpsnr1)]) < match_radius_arcsec \
            #and np.median(distnr[np.isfinite(distnr)]) < match_radius_arcsec \
            #and np.median(sgscore[np.isfinite(sgscore)]) > star_galaxy_threshold    
    


    #def run(self, locus):
        #if locus.alert.properties['ant_survey'] != 1: #be sure we aren't triggered with upper limits
            #print ("up lim")
            #return 
        
        #threshold = 0.5  # in magnitude unit, and same for all filters
        #fid = locus.alert.properties['ztf_fid']

        #df = locus.timeseries
        
        #mask = (df['ztf_fid'] == fid) & (df['ant_survey'] == 1) #expect both columns to be unmasked
        #df = df[mask]  
        
        
        #if len(df) < 2: # Locus has < 2 measurements with fid matching the current alert fid.
            #print ('too few points')
            #return

        #corrected = False
        #mag = df['ztf_magpsf'].data #expect unmasked
        #magerr = df['ztf_sigmapsf'].data #expect unmasked
        
        #if isinstance(df['ant_mag_corrected'], MaskedColumn) == False:
            #corrected = True
            #mag = df['ant_mag_corrected'].data 
            #magerr = df['ant_magerr_corrected'].data 
        #else:
            #if np.sum(df['ant_mag_corrected'].mask) < len(df):
                #corrected = True
                
                #mag = df['ant_mag_corrected'].filled(fill_value=np.nan).data 
                #mm = np.isfinite(mag)
                #mag = mag[mm]
                
                #magerr = df['ant_magerr_corrected'].filled(fill_value=np.nan).data
                #mm = np.isfinite(magerr)
                #magerr = magerr[mm]
                        
        
        #is_var_star = self._is_var_star(df) ## only one filter type passed

        #if is_var_star == True and corrected == True:
            #tag = 'high_amplitude_variable_star_candidate'
        #if is_var_star == True and corrected == False:  #if uncorrected but var_star, we skip
            #return 
        #if is_var_star == False:
            #tag = 'high_amplitude_transient_candidate'

        #alert_id = locus.alert.alert_id  # current alert_id
        #ztf_object_id = locus.properties['ztf_object_id']  # ZTF Object ID
        
        #W = 1.0 / magerr ** 2.0
        #des = DescrStatsW(mag, weights=W)
        ##print (is_var_star, corrected, des.std, des.mean, tag)
        
        #if des.std > threshold:
            ##print (f'hit!!! {tag} {alert_id} {ztf_object_id}')
            #locus.tag(tag)
            
            
            
            
#Output

#When an alert exits the pipeline it has been flagged with catalog matches, 
#arbitrary new data properties generated by the filters, and stream associations. 
#At this point we check alerts for association with user-submitted watched 
#objects, and send Slack notifications accordingly.

#Finally, we output the alert to Kafka streams if it was associated with a 
#stream. Downstream systems and users connect to the streams in real-time using 
#the ANTARES client library.             
            
            
            
            
            
