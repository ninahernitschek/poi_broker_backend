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


	
def main():
	
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
		group="Hernitschek"
		)
	

	#The poll method can be used to retrieve an alert. It returns a (topic, locus) tuple where topic is a string 
	#(in this example either "extragalactic_staging" or "nuclear_transient_staging") and locus is a Locus instance 
	#that contains the history of observations at the alert site. By default, this method will block indefinitely,
	#waiting for an alert. If you pass an argument to the timeout keyword, the method will return (None, None) 
	#after timeout seconds have elapsed:

	topic = 'high_amplitude_variable_star_candidate_staging'
	datetimenow = datetime.datetime.now()
	print(datetimenow)


## TODO: Add logging
	# disables logging from GPy.py unless they are at least WARNING

		#logging.getLogger("GP").setLevel(logging.WARNING)

		#logging.basicConfig(filename='logfiles/%s_%s__stream.log' % (datetimenow,topic),format='%(asctime)s %(message)s',
						#level=logging.INFO)
		#logging.info('start processing alert archive')


	#f = open('logfiles/%s__stream.log' % (topic), "a+")





	current_dirs_parent = os.path.dirname(os.getcwd())
	db_path = current_dirs_parent + '/_broker_db/ztf_alerts_stream.db'
		
	# dbconn = sqlite3.connect(db_path, isolation_level=None)	
	
	
# 	
# 	dbconn.execute('pragma journal_mode=wal;')
# 
# 	c = dbconn.cursor()
# 	
# 	c_2 = dbconn.cursor()	
# 	
# 	c_3 = dbconn.cursor()

	#### make a loop and keep it running
	while True:
	
		print('try connecting ', datetime.datetime.now())
		
		print ( client.iter())
		
		try:
				for topic, locus in client.iter():
					
					print('topic ', topic)
					print('locus ', locus)

		except:
			print('an exception has occurred')
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
            
            
            
            
            
