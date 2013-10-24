import logging
import sys
import json
import entity_type as etype
import time
import threading
import os
import traceback
import subprocess


from CiviEntity import *
from CiviCRM import CiviCRM

class CiviAPIException(Exception):
	pass

class CiviCRM_DRUSH(CiviCRM):

	def __init__(self, folder='.', drush_path='drush', site='default', logfile=None):
		# init some attributes
		CiviCRM.__init__(self, logfile)
		self.folder = os.path.expanduser(folder)
		self.drush_path = os.path.expanduser(drush_path)
		self.site = 'default'
		self.non_parameters = set(['action', 'entity', 'key', 'api_key', 'sequential', 'json'])
		print self.folder


	def performAPICall(self, params=dict()):
		timestamp = time.time()

		# build call with parameters
		call_params = [self.drush_path, '-r', self.folder, '-l', self.site, 'civicrm-api', '--out=json']
		call_params.append(params['entity'] + '.' + params['action'])
		for param in params:
			# FIXME: use --in=json via stdin!
			if not param in self.non_parameters:
				call_params.append(param + '=' + str(params[param]))

		try:
			reply = subprocess.check_output(call_params)
			self.log("API call completed - parameters: '%s'" % str(call_params), 
				logging.DEBUG, 'API', params.get('action', "NO ACTION SET"), params.get('entity', "NO ENTITY SET!"), params.get('id', ''), params.get('external_identifier', ''), time.time()-timestamp)

			result = json.loads(reply)

			# do some logging
			runtime = time.time()-timestamp
			self._api_calls += 1
			self._api_calls_time += runtime

			if result.has_key('undefined_fields'):
				fields = result['undefined_fields']
				if fields:
					self.log("API call: Undefined fields reported: %s" % str(fields), 
						logging.DEBUG, 'API', params['action'], params['entity'], params.get('id', ''), params.get('external_identifier', ''), time.time()-timestamp)

			if result['is_error']:
				self.log("API call error: '%s'" % result['error_message'], 
					logging.ERROR, 'API', params['action'], params['entity'], params.get('id', ''), params.get('external_identifier', ''), time.time()-timestamp)
				raise CiviAPIException(result['error_message'])
			else:
				return result

		except:
			raise CiviAPIException("DRUSH failed! Please check paths.")

