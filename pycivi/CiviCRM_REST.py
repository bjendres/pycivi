import requests
import logging
import sys
import json
import entity_type as etype
import time
import threading
import os
import traceback
from distutils.version import LooseVersion

from CiviEntity import *
from CiviCRM import CiviCRM


if LooseVersion(requests.__version__) < LooseVersion('1.1.0'):
    print "ERROR: You need requests >= 1.1.0"
    print "You've got {0}".format(requests.__version__)
    sys.exit(1)



class CiviAPIException(Exception):
	pass

class CiviCRM_REST(CiviCRM):

	def __init__(self, url, site_key, user_key, logfile=None):
		# init some attributes
		CiviCRM.__init__(self, logfile)
		self.url = url
		self.site_key = site_key
		self.user_key = user_key

		# set rest url
		if self.url.endswith('extern/rest.php'):
			# in this case it's fine, this is the rest URL
			self.rest_url = self.url
		else:
			if self.url.endswith('/civicrm'):
				self.rest_url = self.url[:-8] + '/sites/all/modules/civicrm/extern/rest.php'
			else:
				self.rest_url = self.url + '/sites/all/modules/civicrm/extern/rest.php'


	def performAPICall(self, params=dict()):
		timestamp = time.time()
		params['api_key'] = self.user_key
		params['key'] = self.site_key
		params['sequential'] = 1
		params['json'] = 1
		params['version'] = self.api_version
		if self.debug:
			params['debug'] = 1

		if params['action'] in ['create', 'delete']:
			reply = requests.post(self.rest_url, params=params, verify=False)
		else:
			reply = requests.get(self.rest_url, params=params, verify=False)

		self.log("API call completed - status: %d, url: '%s'" % (reply.status_code, reply.url), 
			logging.DEBUG, 'API', params.get('action', "NO ACTION SET"), params.get('entity', "NO ENTITY SET!"), params.get('id', ''), params.get('external_identifier', ''), time.time()-timestamp)

		if reply.status_code != 200:
			raise CiviAPIException("HTML response code %d received, please check URL" % reply.status_code)

		result = json.loads(reply.text)
				
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


