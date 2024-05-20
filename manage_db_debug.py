#from ingest import do_ingest
#from ztf import db, logger
#import sys
#import requests
#import tarfile
#import base64
from datetime import datetime, timedelta

import os
import io
import numpy as np

#import pandas as pd

#import matplotlib
#matplotlib.use('cairo')

#import matplotlib.pyplot as plt


#import avro

#from avro.datafile import DataFileReader, DataFileWriter
#from avro.io import DatumReader, DatumWriter
#import fastavro

from astropy.time import Time
from astropy.io import fits
#import aplpy

#import gzip
#import re

import sqlite3


	
	
def add_index():
	
	
	
	#outfolder = '/media/nhernits/Elements/'
	

	conn = sqlite3.connect('ztf_alerts_stream.db')
	
	#conn = sqlite3.connect(outfolder+'/ztf_alerts_manual.db')
	
	
	c = conn.cursor()

## Create table
	c.execute('''CREATE TABLE featuretable_2
             (id integer primary key autoincrement, date_log string, alert_id string, locus_id string, locus_ra real, locus_dec real, date_alert_mjd real, 
              ztf_object_id string, num_alerts integer,
		num_mag_values integer, brightest_alert_id_ztf string,         
		brightest_alert_magnitude_ztf real, brightest_alert_observation_time_ztf real,		
		feature_amplitude_magn_r real, feature_anderson_darling_normal_magn_r real,
		feature_beyond_1_std_magn_r real, feature_beyond_2_std_magn_r real, feature_cusum_magn_r real, 
		feature_eta_e_magn_r real, feature_inter_percentile_range_2_magn_r real,
		feature_inter_percentile_range_10_magn_r real, feature_inter_percentile_range_25_magn_r real, 
		feature_kurtosis_magn_r real, feature_linear_fit_slope_magn_r real, feature_linear_fit_slope_sigma_magn_r real, 
		feature_linear_fit_reduced_chi2_magn_r real, feature_linear_trend_magn_r real, feature_linear_trend_sigma_magn_r real, 
		feature_magnitude_percentage_ratio_40_5_magn_r real, feature_magnitude_percentage_ratio_20_5_magn_r real, 
		feature_maximum_slope_magn_r real, feature_mean_magn_r real, feature_median_absolute_deviation_magn_r real, 
		feature_percent_amplitude_magn_r real, feature_percent_difference_magnitude_percentile_5_magn_r real, 
		feature_percent_difference_magnitude_percentile_10_magn_r real, feature_median_buffer_range_percentage_10_magn_r real, 
		feature_median_buffer_range_percentage_20_magn_r real, feature_period_0_magn_r real, 
		feature_period_s_to_n_0_magn_r real, feature_period_1_magn_r real, feature_period_s_to_n_1_magn_r real, 
		feature_period_2_magn_r real, feature_period_s_to_n_2_magn_r real, feature_period_3_magn_r real, 
		feature_period_s_to_n_3_magn_r real, feature_period_4_magn_r real, feature_period_s_to_n_4_magn_r real, 
		feature_periodogram_amplitude_magn_r real, feature_periodogram_beyond_2_std_magn_r real, 
		feature_periodogram_beyond_3_std_magn_r real, feature_periodogram_standard_deviation_magn_r real, 
		feature_chi2_magn_r real, feature_skew_magn_r real, feature_standard_deviation_magn_r real, 
		feature_stetson_k_magn_r real, feature_weighted_mean_magn_r real, feature_anderson_darling_normal_flux_r real, 
		feature_cusum_flux_r real, feature_eta_e_flux_r real, feature_excess_variance_flux_r real, 
		feature_kurtosis_flux_r real, feature_mean_variance_flux_r real, feature_chi2_flux_r real, feature_skew_flux_r real, 
		feature_stetson_k_flux_r real, feature_amplitude_magn_g real, feature_anderson_darling_normal_magn_g real, 
		feature_beyond_1_std_magn_g real, feature_beyond_2_std_magn_g real, feature_cusum_magn_g real, 
		feature_eta_e_magn_g real, feature_inter_percentile_range_2_magn_g real, feature_inter_percentile_range_10_magn_g real, 
		feature_inter_percentile_range_25_magn_g real, feature_kurtosis_magn_g real, feature_linear_fit_slope_magn_g real, 
		feature_linear_fit_slope_sigma_magn_g real, feature_linear_fit_reduced_chi2_magn_g real, 
		feature_linear_trend_magn_g real, feature_linear_trend_sigma_magn_g real, 
		feature_magnitude_percentage_ratio_40_5_magn_g real, feature_magnitude_percentage_ratio_20_5_magn_g real, 
		feature_maximum_slope_magn_g real, feature_mean_magn_g real, feature_median_absolute_deviation_magn_g real, 
		feature_percent_amplitude_magn_g real, feature_percent_difference_magnitude_percentile_5_magn_g real, 
		feature_percent_difference_magnitude_percentile_10_magn_g real, feature_median_buffer_range_percentage_10_magn_g real, 
		feature_median_buffer_range_percentage_20_magn_g real, feature_period_0_magn_g real, feature_period_s_to_n_0_magn_g real, 
		feature_period_1_magn_g real, feature_period_s_to_n_1_magn_g real, feature_period_2_magn_g real, 
		feature_period_s_to_n_2_magn_g real, feature_period_3_magn_g real, feature_period_s_to_n_3_magn_g real, 
		feature_period_4_magn_g real, feature_period_s_to_n_4_magn_g real, feature_periodogram_amplitude_magn_g real, 
		feature_periodogram_beyond_2_std_magn_g real, feature_periodogram_beyond_3_std_magn_g real, 
		feature_periodogram_standard_deviation_magn_g real, feature_chi2_magn_g real, feature_skew_magn_g real, 
		feature_standard_deviation_magn_g real, feature_stetson_k_magn_g real, feature_weighted_mean_magn_g real, 
		feature_anderson_darling_normal_flux_g real, feature_cusum_flux_g real, feature_eta_e_flux_g real, 
		feature_excess_variance_flux_g real, feature_kurtosis_flux_g real, feature_mean_variance_flux_g real, 
		feature_chi2_flux_g real, feature_skew_flux_g real, feature_stetson_k_flux_g real, anomaly_score real, anomaly_mask string, anomaly_type string, is_corrected bool)''')




	c.execute('''insert into featuretable_2(date_log, alert_id, locus_id, locus_ra, locus_dec, date_alert_mjd, 
              ztf_object_id, num_alerts,
		num_mag_values, brightest_alert_id_ztf,         
		brightest_alert_magnitude_ztf, brightest_alert_observation_time_ztf,		
		feature_amplitude_magn_r, feature_anderson_darling_normal_magn_r,
		feature_beyond_1_std_magn_r, feature_beyond_2_std_magn_r, feature_cusum_magn_r, 
		feature_eta_e_magn_r, feature_inter_percentile_range_2_magn_r,
		feature_inter_percentile_range_10_magn_r, feature_inter_percentile_range_25_magn_r, 
		feature_kurtosis_magn_r, feature_linear_fit_slope_magn_r, feature_linear_fit_slope_sigma_magn_r, 
		feature_linear_fit_reduced_chi2_magn_r, feature_linear_trend_magn_r, feature_linear_trend_sigma_magn_r, 
		feature_magnitude_percentage_ratio_40_5_magn_r, feature_magnitude_percentage_ratio_20_5_magn_r, 
		feature_maximum_slope_magn_r, feature_mean_magn_r, feature_median_absolute_deviation_magn_r, 
		feature_percent_amplitude_magn_r, feature_percent_difference_magnitude_percentile_5_magn_r, 
		feature_percent_difference_magnitude_percentile_10_magn_r, feature_median_buffer_range_percentage_10_magn_r, 
		feature_median_buffer_range_percentage_20_magn_r, feature_period_0_magn_r, 
		feature_period_s_to_n_0_magn_r, feature_period_1_magn_r, feature_period_s_to_n_1_magn_r, 
		feature_period_2_magn_r, feature_period_s_to_n_2_magn_r, feature_period_3_magn_r, 
		feature_period_s_to_n_3_magn_r, feature_period_4_magn_r, feature_period_s_to_n_4_magn_r, 
		feature_periodogram_amplitude_magn_r, feature_periodogram_beyond_2_std_magn_r, 
		feature_periodogram_beyond_3_std_magn_r, feature_periodogram_standard_deviation_magn_r, 
		feature_chi2_magn_r, feature_skew_magn_r, feature_standard_deviation_magn_r, 
		feature_stetson_k_magn_r, feature_weighted_mean_magn_r, feature_anderson_darling_normal_flux_r, 
		feature_cusum_flux_r, feature_eta_e_flux_r, feature_excess_variance_flux_r, 
		feature_kurtosis_flux_r, feature_mean_variance_flux_r, feature_chi2_flux_r, feature_skew_flux_r, 
		feature_stetson_k_flux_r, feature_amplitude_magn_g, feature_anderson_darling_normal_magn_g, 
		feature_beyond_1_std_magn_g, feature_beyond_2_std_magn_g, feature_cusum_magn_g, 
		feature_eta_e_magn_g, feature_inter_percentile_range_2_magn_g, feature_inter_percentile_range_10_magn_g, 
		feature_inter_percentile_range_25_magn_g, feature_kurtosis_magn_g, feature_linear_fit_slope_magn_g, 
		feature_linear_fit_slope_sigma_magn_g, feature_linear_fit_reduced_chi2_magn_g, 
		feature_linear_trend_magn_g, feature_linear_trend_sigma_magn_g, 
		feature_magnitude_percentage_ratio_40_5_magn_g, feature_magnitude_percentage_ratio_20_5_magn_g, 
		feature_maximum_slope_magn_g, feature_mean_magn_g, feature_median_absolute_deviation_magn_g, 
		feature_percent_amplitude_magn_g, feature_percent_difference_magnitude_percentile_5_magn_g, 
		feature_percent_difference_magnitude_percentile_10_magn_g, feature_median_buffer_range_percentage_10_magn_g, 
		feature_median_buffer_range_percentage_20_magn_g, feature_period_0_magn_g, feature_period_s_to_n_0_magn_g, 
		feature_period_1_magn_g, feature_period_s_to_n_1_magn_g, feature_period_2_magn_g, 
		feature_period_s_to_n_2_magn_g, feature_period_3_magn_g, feature_period_s_to_n_3_magn_g, 
		feature_period_4_magn_g, feature_period_s_to_n_4_magn_g, feature_periodogram_amplitude_magn_g, 
		feature_periodogram_beyond_2_std_magn_g, feature_periodogram_beyond_3_std_magn_g, 
		feature_periodogram_standard_deviation_magn_g, feature_chi2_magn_g, feature_skew_magn_g, 
		feature_standard_deviation_magn_g, feature_stetson_k_magn_g, feature_weighted_mean_magn_g, 
		feature_anderson_darling_normal_flux_g, feature_cusum_flux_g, feature_eta_e_flux_g, 
		feature_excess_variance_flux_g, feature_kurtosis_flux_g, feature_mean_variance_flux_g, 
		feature_chi2_flux_g, feature_skew_flux_g, feature_stetson_k_flux_g, anomaly_score, 
		anomaly_mask, anomaly_type, is_corrected) 
		select date_log, alert_id, locus_id, locus_ra, locus_dec, date_alert_mjd, 
              ztf_object_id, num_alerts,
		num_mag_values, brightest_alert_id_ztf,         
		brightest_alert_magnitude_ztf, brightest_alert_observation_time_ztf,		
		feature_amplitude_magn_r, feature_anderson_darling_normal_magn_r,
		feature_beyond_1_std_magn_r, feature_beyond_2_std_magn_r, feature_cusum_magn_r, 
		feature_eta_e_magn_r, feature_inter_percentile_range_2_magn_r,
		feature_inter_percentile_range_10_magn_r, feature_inter_percentile_range_25_magn_r, 
		feature_kurtosis_magn_r, feature_linear_fit_slope_magn_r, feature_linear_fit_slope_sigma_magn_r, 
		feature_linear_fit_reduced_chi2_magn_r, feature_linear_trend_magn_r, feature_linear_trend_sigma_magn_r, 
		feature_magnitude_percentage_ratio_40_5_magn_r, feature_magnitude_percentage_ratio_20_5_magn_r, 
		feature_maximum_slope_magn_r, feature_mean_magn_r, feature_median_absolute_deviation_magn_r, 
		feature_percent_amplitude_magn_r, feature_percent_difference_magnitude_percentile_5_magn_r, 
		feature_percent_difference_magnitude_percentile_10_magn_r, feature_median_buffer_range_percentage_10_magn_r, 
		feature_median_buffer_range_percentage_20_magn_r, feature_period_0_magn_r, 
		feature_period_s_to_n_0_magn_r, feature_period_1_magn_r, feature_period_s_to_n_1_magn_r, 
		feature_period_2_magn_r, feature_period_s_to_n_2_magn_r, feature_period_3_magn_r, 
		feature_period_s_to_n_3_magn_r, feature_period_4_magn_r, feature_period_s_to_n_4_magn_r, 
		feature_periodogram_amplitude_magn_r, feature_periodogram_beyond_2_std_magn_r, 
		feature_periodogram_beyond_3_std_magn_r, feature_periodogram_standard_deviation_magn_r, 
		feature_chi2_magn_r, feature_skew_magn_r, feature_standard_deviation_magn_r, 
		feature_stetson_k_magn_r, feature_weighted_mean_magn_r, feature_anderson_darling_normal_flux_r, 
		feature_cusum_flux_r, feature_eta_e_flux_r, feature_excess_variance_flux_r, 
		feature_kurtosis_flux_r, feature_mean_variance_flux_r, feature_chi2_flux_r, feature_skew_flux_r, 
		feature_stetson_k_flux_r, feature_amplitude_magn_g, feature_anderson_darling_normal_magn_g, 
		feature_beyond_1_std_magn_g, feature_beyond_2_std_magn_g, feature_cusum_magn_g, 
		feature_eta_e_magn_g, feature_inter_percentile_range_2_magn_g, feature_inter_percentile_range_10_magn_g, 
		feature_inter_percentile_range_25_magn_g, feature_kurtosis_magn_g, feature_linear_fit_slope_magn_g, 
		feature_linear_fit_slope_sigma_magn_g, feature_linear_fit_reduced_chi2_magn_g, 
		feature_linear_trend_magn_g, feature_linear_trend_sigma_magn_g, 
		feature_magnitude_percentage_ratio_40_5_magn_g, feature_magnitude_percentage_ratio_20_5_magn_g, 
		feature_maximum_slope_magn_g, feature_mean_magn_g, feature_median_absolute_deviation_magn_g, 
		feature_percent_amplitude_magn_g, feature_percent_difference_magnitude_percentile_5_magn_g, 
		feature_percent_difference_magnitude_percentile_10_magn_g, feature_median_buffer_range_percentage_10_magn_g, 
		feature_median_buffer_range_percentage_20_magn_g, feature_period_0_magn_g, feature_period_s_to_n_0_magn_g, 
		feature_period_1_magn_g, feature_period_s_to_n_1_magn_g, feature_period_2_magn_g, 
		feature_period_s_to_n_2_magn_g, feature_period_3_magn_g, feature_period_s_to_n_3_magn_g, 
		feature_period_4_magn_g, feature_period_s_to_n_4_magn_g, feature_periodogram_amplitude_magn_g, 
		feature_periodogram_beyond_2_std_magn_g, feature_periodogram_beyond_3_std_magn_g, 
		feature_periodogram_standard_deviation_magn_g, feature_chi2_magn_g, feature_skew_magn_g, 
		feature_standard_deviation_magn_g, feature_stetson_k_magn_g, feature_weighted_mean_magn_g, 
		feature_anderson_darling_normal_flux_g, feature_cusum_flux_g, feature_eta_e_flux_g, 
		feature_excess_variance_flux_g, feature_kurtosis_flux_g, feature_mean_variance_flux_g, 
		feature_chi2_flux_g, feature_skew_flux_g, feature_stetson_k_flux_g, anomaly_score, 
		anomaly_mask, anomaly_type, is_corrected from featuretable''')
	
	
	c.execute('drop table featuretable')
	
	c.execute('alter table featuretable_2 rename to featuretable')
	
	#c.execute('drop table events')
	#c.execute('alter table events_copy rename to events')



	#c.execute('SELECT * FROM indextable')
	#c.execute('SELECT * FROM featuretable')
	#print(c.fetchall())
	 
	
	#print(c.fetchone())

	#date=20201202
	#candid=1430091596315010014
	#objectId='ZTF18abshabg'
	#jd=2459184.59159720
	#filter=2
	#ra=317.10938500
	#dec=-14.73110590
	#magpsf=18.86895370
	#magap=19.20140076
	
	

# Create a secondary key on the name column

	#createSecondaryIndex = "CREATE INDEX index_candid ON indextable(candid)"

	#c.execute(createSecondaryIndex)
	
	#c.execute("insert into indextable values (?,?,?,?,?,?,?,?,?)",(date,candid,objectId,jd,filter,ra,dec,magpsf,magap)) 

	# Save (commit) the changes
	conn.commit()


	#c.execute('SELECT * FROM indextable')
	#print(c.fetchone())


	# We can also close the connection if we are done with it.
	# Just be sure any changes have been committed or they will be lost.
	conn.close()




def create_db():
	
	

	conn = sqlite3.connect('ztf_alerts_stream.db')
	
	#conn = sqlite3.connect(outfolder+'/ztf_alerts_manual.db')
	
	
	c = conn.cursor()

## Create table

	c.execute('''CREATE TABLE featuretable
             (id integer primary key autoincrement, date_log string, alert_id string, locus_id string, locus_ra real, locus_dec real, date_alert_mjd real, 
              ztf_object_id string, num_alerts integer,
		num_mag_values integer, brightest_alert_id_ztf string,         
		brightest_alert_magnitude_ztf real, brightest_alert_observation_time_ztf real,		
		feature_amplitude_magn_r real, feature_anderson_darling_normal_magn_r real,
		feature_beyond_1_std_magn_r real, feature_beyond_2_std_magn_r real, feature_cusum_magn_r real, 
		feature_eta_e_magn_r real, feature_inter_percentile_range_2_magn_r real,
		feature_inter_percentile_range_10_magn_r real, feature_inter_percentile_range_25_magn_r real, 
		feature_kurtosis_magn_r real, feature_linear_fit_slope_magn_r real, feature_linear_fit_slope_sigma_magn_r real, 
		feature_linear_fit_reduced_chi2_magn_r real, feature_linear_trend_magn_r real, feature_linear_trend_sigma_magn_r real, 
		feature_magnitude_percentage_ratio_40_5_magn_r real, feature_magnitude_percentage_ratio_20_5_magn_r real, 
		feature_maximum_slope_magn_r real, feature_mean_magn_r real, feature_median_absolute_deviation_magn_r real, 
		feature_percent_amplitude_magn_r real, feature_percent_difference_magnitude_percentile_5_magn_r real, 
		feature_percent_difference_magnitude_percentile_10_magn_r real, feature_median_buffer_range_percentage_10_magn_r real, 
		feature_median_buffer_range_percentage_20_magn_r real, feature_period_0_magn_r real, 
		feature_period_s_to_n_0_magn_r real, feature_period_1_magn_r real, feature_period_s_to_n_1_magn_r real, 
		feature_period_2_magn_r real, feature_period_s_to_n_2_magn_r real, feature_period_3_magn_r real, 
		feature_period_s_to_n_3_magn_r real, feature_period_4_magn_r real, feature_period_s_to_n_4_magn_r real, 
		feature_periodogram_amplitude_magn_r real, feature_periodogram_beyond_2_std_magn_r real, 
		feature_periodogram_beyond_3_std_magn_r real, feature_periodogram_standard_deviation_magn_r real, 
		feature_chi2_magn_r real, feature_skew_magn_r real, feature_standard_deviation_magn_r real, 
		feature_stetson_k_magn_r real, feature_weighted_mean_magn_r real, feature_anderson_darling_normal_flux_r real, 
		feature_cusum_flux_r real, feature_eta_e_flux_r real, feature_excess_variance_flux_r real, 
		feature_kurtosis_flux_r real, feature_mean_variance_flux_r real, feature_chi2_flux_r real, feature_skew_flux_r real, 
		feature_stetson_k_flux_r real, feature_amplitude_magn_g real, feature_anderson_darling_normal_magn_g real, 
		feature_beyond_1_std_magn_g real, feature_beyond_2_std_magn_g real, feature_cusum_magn_g real, 
		feature_eta_e_magn_g real, feature_inter_percentile_range_2_magn_g real, feature_inter_percentile_range_10_magn_g real, 
		feature_inter_percentile_range_25_magn_g real, feature_kurtosis_magn_g real, feature_linear_fit_slope_magn_g real, 
		feature_linear_fit_slope_sigma_magn_g real, feature_linear_fit_reduced_chi2_magn_g real, 
		feature_linear_trend_magn_g real, feature_linear_trend_sigma_magn_g real, 
		feature_magnitude_percentage_ratio_40_5_magn_g real, feature_magnitude_percentage_ratio_20_5_magn_g real, 
		feature_maximum_slope_magn_g real, feature_mean_magn_g real, feature_median_absolute_deviation_magn_g real, 
		feature_percent_amplitude_magn_g real, feature_percent_difference_magnitude_percentile_5_magn_g real, 
		feature_percent_difference_magnitude_percentile_10_magn_g real, feature_median_buffer_range_percentage_10_magn_g real, 
		feature_median_buffer_range_percentage_20_magn_g real, feature_period_0_magn_g real, feature_period_s_to_n_0_magn_g real, 
		feature_period_1_magn_g real, feature_period_s_to_n_1_magn_g real, feature_period_2_magn_g real, 
		feature_period_s_to_n_2_magn_g real, feature_period_3_magn_g real, feature_period_s_to_n_3_magn_g real, 
		feature_period_4_magn_g real, feature_period_s_to_n_4_magn_g real, feature_periodogram_amplitude_magn_g real, 
		feature_periodogram_beyond_2_std_magn_g real, feature_periodogram_beyond_3_std_magn_g real, 
		feature_periodogram_standard_deviation_magn_g real, feature_chi2_magn_g real, feature_skew_magn_g real, 
		feature_standard_deviation_magn_g real, feature_stetson_k_magn_g real, feature_weighted_mean_magn_g real, 
		feature_anderson_darling_normal_flux_g real, feature_cusum_flux_g real, feature_eta_e_flux_g real, 
		feature_excess_variance_flux_g real, feature_kurtosis_flux_g real, feature_mean_variance_flux_g real, 
		feature_chi2_flux_g real, feature_skew_flux_g real, feature_stetson_k_flux_g real, anomaly_score real, anomaly_mask string, 
		anomaly_type string, is_corrected bool, ant_mag_corrected real, ant_passband real)''')

				
	
	createSecondaryIndex = "CREATE INDEX index_alert_id ON featuretable(alert_id)"
	
	


	conn.commit()
	conn.close()





			
if __name__ == '__main__':

	# CREATE THE DATABASE
	create_db()
	
	

