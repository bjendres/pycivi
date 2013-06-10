import requests
import logging
import sys
import json
import entity_type as etype
import time

from CiviEntity import *

class CiviAPIException(Exception):
	pass

class CiviCRM:

	def __init__(self, url, site_key, user_key):
		# init some attributes
		self.url = url
		self.site_key = site_key
		self.user_key = user_key

		# set up logging
		self.logger = logging.getLogger('pycivi')
		self.logger.setLevel(logging.INFO)
		self.logger.addHandler(logging.StreamHandler(sys.stdout))

		# some more internal attributes
		self.debug = False
		self.api_version = 3
		self._api_calls = 0
		self._api_calls_time = 0.0

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
			reply = requests.post(self.rest_url, params=params)
		else:
			reply = requests.get(self.rest_url, params=params)

		self.logger.debug("Calling URL: %s" % reply.url)

		if reply.status_code != 200:
			raise CiviAPIException("HTML response code %d received, please check URL" % reply.status_code)

		result = json.loads(reply.text)
		
		# do some logging
		runtime = time.time()-timestamp
		self._api_calls += 1
		self._api_calls_time += runtime
		self.logger.debug("API call took %dms" % int(runtime*1000))

		if result.has_key('undefined_fields'):
			fields = result['undefined_fields']
			if fields:
				self.logger.debug("Undefined fields reported: %s" % str(fields))

		if result['is_error']:
			raise CiviAPIException(result['error_message'])
		else:
			return result


	def probe(self):
		# check by calling get contact
		try:
			self.performAPICall({'entity':'Contact', 'action':'get', 'option.limit':1})
			return True
		except:
			return False


	def load(self, entity_type, entity_id):
		result = self.performAPICall({'entity':entity_type, 'action':'get', 'id':entity_id})
		if result['count']:
			return self._createEntity(entity_type, result['values'][0])
		else:
			return None


	def getOrCreate(self, entity_type, attributes):
		query = dict(attributes)
		query['entity'] = entity_type
		query['action'] = 'get'
		result = self.performAPICall(query)

		if result['count']>1:
			raise CiviAPIException("Query result not unique, please provide a unique query for 'getOrCreate'.")
		elif result['count']==1:
			return self._createEntity(entity_type, result['values'][0])
		else:
			query['action'] = 'create'
			result = self.performAPICall(query)
			return self._createEntity(entity_type, result['values'][0])


	def _createEntity(self, entity_type, attributes):
		if entity_type==etype.CONTACT:
			return CiviContactEntity(entity_type, attributes['id'], self, attributes)
		else:
			return CiviEntity(entity_type, attributes['id'], self, attributes)

