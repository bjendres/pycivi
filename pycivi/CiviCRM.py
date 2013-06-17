import requests
import logging
import sys
import json
import entity_type as etype
import time
import threading

from CiviEntity import *

class CiviAPIException(Exception):
	pass

class CiviCRM:

	def __init__(self, url, site_key, user_key, logfile=None):
		# init some attributes
		self.url = url
		self.site_key = site_key
		self.user_key = user_key

		self.lookup_cache = dict()
		self.lookup_cache_lock = threading.Condition()

		# set up logging
		self.logger_format = u"%(type)s;%(entity_type)s;%(first_id)s;%(second_id)s;%(duration)sms;%(thread_id)s;%(text)s"
		self._logger = logging.getLogger('pycivi')
		self._logger.setLevel(logging.DEBUG)

		# add the console logger
		logger1 = logging.StreamHandler()
		logger1.setLevel(logging.INFO)
		class MessageOnly(logging.Formatter):
			def format(self, record):
				return logging.Formatter.format(self, record).split(';')[-1]
		logger1.setFormatter(MessageOnly())
		self._logger.addHandler(logger1)

		# add the file logger
		if logfile:
			logger2 = logging.FileHandler(logfile)
			logger2.setLevel(logging.DEBUG)
			logger2.setFormatter(logging.Formatter(u'%(asctime)s;%(message)s'))
			self._logger.addHandler(logger2)
		
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



	def log(self, message, level=logging.INFO, type='Unknown', command='Unknown', entity_type='', first_id='', second_id='', duration='n/a'):
		"""
		formally log information.
		"""
		self._logger.log(level, self.logger_format,  { 	'type': type,
														'command': command,
														'entity_type': entity_type,
														'first_id': first_id,
														'second_id': second_id,
														'duration': str(int(duration * 1000)),
														'thread_id': threading.current_thread().ident,
														'text': message,
													})
		


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
				logging.DEBUG, 'API', params['action'], params['entity'], params.get('id', ''), params.get('external_identifier', ''), time.time()-timestamp)
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

	

	def getEntity(self, entity_type, attributes, primary_attributes=['external_identifier']):
		timestamp = time.time()
		if attributes.has_key('id'):
			return attributes['id']
		elif attributes.has_key('contact_id'):
			return attributes['contact_id']

		query = dict()
		first_key = None
		for key in primary_attributes: 
			if attributes.has_key(key):
				query[key] = attributes[key]
				if first_key==None:
					first_key = attributes[key]
		if not len(query) > 0:
			self.log("No primary key provided with contact '%s'." % str(attributes),
				logging.DEBUG, 'pycivi', 'get', 'Contact', first_key, None, time.time()-timestamp)
			return 0

		query['entity'] = entity_type
		query['action'] = 'get'
		result = self.performAPICall(query)
		if result['is_error']:
			raise CiviAPIException(result['error_message'])
		if result['count']>1:
			self.log("Query result not unique, please provide a unique query for 'getEntity'.",
				logging.WARN, 'pycivi', 'get', entity_type, first_key, None, time.time()-timestamp)
			raise CiviAPIException("Query result not unique, please provide a unique query for 'getEntity'.")
		elif result['count']==1:
			entity = self._createEntity(entity_type, result['values'][0])
			self.log("Entity found: %s" % unicode(str(entity), 'utf8'),
				logging.DEBUG, 'pycivi', 'get', entity_type, first_key, None, time.time()-timestamp)
			return entity
		else:
			self.log("Entity not found.",
				logging.DEBUG, 'pycivi', 'get', entity_type, first_key, None, time.time()-timestamp)
			return None




	###########################################################################
	#                            Lookup methods                               #
	###########################################################################


	def getContactID(self, attributes, primary_attributes=['external_identifier']):
		timestamp = time.time()
		if attributes.has_key('id'):
			return attributes['id']
		elif attributes.has_key('contact_id'):
			return attributes['contact_id']
		
		query = dict()
		first_key = None
		for key in primary_attributes: 
			if attributes.has_key(key):
				query[key] = attributes[key]
				if first_key==None:
					first_key = attributes[key]
		if not len(query) > 0:
			self.log("No primary key provided with contact '%s'." % str(attributes),
				logging.DEBUG, 'pycivi', 'get', 'Contact', first_key, None, time.time()-timestamp)
			return 0

		query['entity'] = 'Contact'
		query['action'] = 'get'
		query['return'] = 'contact_id'

		result = self.performAPICall(query)
		if result['count']>1:
			self.log("Query result not unique, please provide a unique query for 'getOrCreate'.",
				logging.WARN, 'pycivi', 'get', 'Contact', first_key, None, time.time()-timestamp)
			raise CiviAPIException("Query result not unique, please provide a unique query for 'getOrCreate'.")
		elif result['count']==1:
			contact_id = result['values'][0]['contact_id']
			self.log("Contact ID resolved.",
				logging.DEBUG, 'pycivi', 'get', 'Contact', first_key, None, time.time()-timestamp)
			return contact_id
		else:
			self.log("Contact not found.",
				logging.DEBUG, 'pycivi', 'get', 'Contact', first_key, None, time.time()-timestamp)
			return 0

	
	def getCustomFieldID(self, field_name, entity_type='Contact'):
		"""
		Get the ID for a given custom field
		"""
		timestamp = time.time()
		if self.lookup_cache.has_key('custom_field') and self.lookup_cache['custom_field'].has_key(field_name):
			return self.lookup_cache['custom_field'][field_name]

		query = dict()
		query['entity'] = 'CustomField'
		query['action'] = 'get'
		query['label'] = field_name

		result = self.performAPICall(query)
		if result['is_error']:
			raise CiviAPIException(result['error_message'])
		if result['count']>1:
			field_id = 0
			self.log(u"More than one custom field found with name '%s'!" % field_name.encode('utf8'),
				logging.WARN, 'API', 'get', 'CustomField', None, None, time.time()-timestamp)
		elif result['count']==0:
			field_id = 0
			self.log(u"Custom field '%s' does not exist." % field_name.encode('utf8'),
				logging.DEBUG, 'API', 'get', 'CustomField', None, None, time.time()-timestamp)
		else:
			field_id = result['values'][0]['id']
			self.log(u"Custom field '%s' resolved to ID %s" % (field_name, field_id),
				logging.DEBUG, 'API', 'get', 'CustomField', field_id, None, time.time()-timestamp)

		# store value
		self.lookup_cache_lock.acquire()
		if not self.lookup_cache.has_key('custom_field'):
			self.lookup_cache['custom_field'] = dict()
		self.lookup_cache['custom_field'][field_name] = field_id
		self.lookup_cache_lock.notifyAll()
		self.lookup_cache_lock.release()

		return field_id


	def setCustomFieldID(self, entity_id, field_name, value):
		"""
		Get the ID for a given custom field
		"""
		timestamp = time.time()
		field_id = self.getCustomFieldID(field_name)
		if not field_id:
			self.log(u"Custom field '%s' does not exist." % field_name.encode('utf8'),
				logging.WARN, 'API', 'get', 'CustomField', None, None, time.time()-timestamp)
			return

		query = dict()
		query['entity'] = 'CustomValue'
		query['action'] = 'get'
		query['entity_id'] = entity_id
		query['label'] = field_name

		result = self.performAPICall(query)
		if result['is_error']:
			raise CiviAPIException(result['error_message'])
		if result['count']>1:
			field_id = 0
			self.log(u"More than one custom field found with name '%s'!" % field_name.encode('utf8'),
				logging.WARN, 'API', 'get', 'CustomField', None, None, time.time()-timestamp)
		elif result['count']==0:
			field_id = 0
			self.log(u"Custom field '%s' does not exist." % field_name.encode('utf8'),
				logging.DEBUG, 'API', 'get', 'CustomField', None, None, time.time()-timestamp)
		else:
			field_id = result['values'][0]['id']
			self.log(u"Custom field '%s' resolved to ID %s" % (unicode(field_name, 'utf8'), field_id),
				logging.DEBUG, 'API', 'get', 'CustomField', field_id, None, time.time()-timestamp)

		# store value
		self.lookup_cache_lock.acquire()
		if not self.lookup_cache.has_key('custom_field'):
			self.lookup_cache['custom_field'] = dict()
		self.lookup_cache['custom_field'][field_name] = field_id
		self.lookup_cache_lock.notifyAll()
		self.lookup_cache_lock.release()

		return field_id


	def getOptionGroupID(self, group_name):
		"""
		Get the ID for a given option group
		"""
		timestamp = time.time()
		if self.lookup_cache.has_key('option_group') and self.lookup_cache['option_group'].has_key(group_name):
			return self.lookup_cache['option_group'][group_name]

		query = dict()
		query['entity'] = 'OptionGroup'
		query['action'] = 'get'
		query['name'] = group_name
		result = self.performAPICall(query)
		if result['is_error']:
			raise CiviAPIException(result['error_message'])
		if result['count']>1:
			group_id = 0
			self.log("More than one group found with name '%s'!" % group_name,
				logging.WARN, 'API', 'get', 'OptionGroup', None, None, time.time()-timestamp)
		elif result['count']==0:
			group_id = 0
			self.log("Group '%s' does not exist." % group_name,
				logging.DEBUG, 'API', 'get', 'OptionGroup', group_id, None, time.time()-timestamp)
		else:
			group_id = result['values'][0]['id']
			self.log("Group '%s' resolved to ID %s" % (group_name, group_id),
				logging.DEBUG, 'API', 'get', 'OptionGroup', group_id, None, time.time()-timestamp)

		# store value
		self.lookup_cache_lock.acquire()
		if not self.lookup_cache.has_key('option_group'):
			self.lookup_cache['option_group'] = dict()
		self.lookup_cache['option_group'][group_name] = group_id
		self.lookup_cache_lock.notifyAll()
		self.lookup_cache_lock.release()

		return group_id

	
	def getOptionValueID(self, option_group_id, name):
		"""
		Get the ID for a given option value
		"""
		timestamp = time.time()
		if self.lookup_cache.has_key('option_value') and self.lookup_cache['option_value'].has_key(option_group_id) and self.lookup_cache['option_value'][option_group_id].has_key(name):
			return self.lookup_cache['option_value'][option_group_id][name]

		query = dict()
		query['entity'] = 'OptionValue'
		query['action'] = 'get'
		query['name'] = name
		query['option_group_id'] = option_group_id
		result = self.performAPICall(query)
		if result['is_error']:
			raise CiviAPIException(result['error_message'])
		if result['count']>1:
			value_id = 0
			self.log("More than one value found with name '%s'!" % name,
				logging.WARN, 'API', 'get', 'OptionValue', None, None, time.time()-timestamp)
		elif result['count']==0:
			value_id = 0
			self.log("Value '%s' does not exist." % name,
				logging.DEBUG, 'API', 'get', 'OptionValue', value_id, None, time.time()-timestamp)
		else:
			value_id = result['values'][0]['value']
			self.log("Value '%s' resolved to ID %s" % (name, value_id),
				logging.DEBUG, 'API', 'get', 'OptionValue', value_id, None, time.time()-timestamp)

		# store value
		self.lookup_cache_lock.acquire()
		if not self.lookup_cache.has_key('option_value'):
			self.lookup_cache['option_value'] = dict()
		if not self.lookup_cache['option_value'].has_key(option_group_id):
			self.lookup_cache['option_value'][option_group_id] = dict()			
		self.lookup_cache['option_value'][option_group_id][name] = value_id
		self.lookup_cache_lock.notifyAll()
		self.lookup_cache_lock.release()

		return value_id


	def setOptionValue(self, option_group_id, name, attributes=dict()):
		"""
		Set or update the value for the given option group
		"""
		timestamp = time.time()
		query = dict(attributes)
		query['action'] = 'create'
		query['entity'] = 'OptionValue'
		query['option_group_id'] = option_group_id
		query['name'] = name
		result = self.performAPICall(query)
		if result['is_error']:
			raise CiviAPIException(result['error_message'])
		
		# store value
		value_id = result['value']
		self.lookup_cache_lock.acquire()
		if not self.lookup_cache.has_key('option_value'):
			self.lookup_cache['option_value'] = dict()
		if not self.lookup_cache['option_value'].has_key(option_group_id):
			self.lookup_cache['option_value'][option_group_id] = dict()			
		self.lookup_cache['option_value'][option_group_id][name] = value_id
		self.lookup_cache_lock.notifyAll()
		self.lookup_cache_lock.release()

		return value_id


	def getLocationTypeID(self, location_name):
		# first: look up in cache
		if self.lookup_cache.has_key('location_type2id') and self.lookup_cache['location_type2id'].has_key(location_name):
			return self.lookup_cache['location_type2id'][location_name]

		timestamp = time.time()
		query = { 	'action': 'get',
					'entity': 'LocationType',
					'name': location_name }
		result = self.performAPICall(query)
		if result['count']>1:
			self.log("Query result not unique, please provide a unique query for 'getOrCreate'.",
				logging.WARN, 'API', 'get', 'LocationType', None, None, time.time()-timestamp)
			raise CiviAPIException("Query result not unique, please provide a unique query for 'getOrCreate'.")
		elif result['count']==1:
			location_id = result['values'][0]['id']
			self.log("Location type '%s' resolved to id %s." % (location_name, location_id),
				logging.DEBUG, 'API', 'get', 'LocationType', location_id, None, time.time()-timestamp)
		else:
			location_id = 0
			self.log("Location type '%s' resolved to id %s." % (location_name, location_id),
				logging.ERROR, 'API', 'get', 'LocationType', location_id, None, time.time()-timestamp)
		self.lookup_cache_lock.acquire()
		if not self.lookup_cache.has_key('location_type2id'):
			self.lookup_cache['location_type2id'] = dict()
		self.lookup_cache['location_type2id'][location_name] = location_id
		self.lookup_cache_lock.notifyAll()
		self.lookup_cache_lock.release()
		return location_id




	def getEmail(self, contact_id, location_type_id):
		timestamp = time.time()
		query = dict()
		query['action']			 	= 'get'
		query['entity'] 			= 'Email'
		query['contact_id'] 		= contact_id
		query['location_type_id'] 	= location_type_id
		result = self.performAPICall(query)
		if result['is_error']:
			raise CiviAPIException(result['error_message'])
		if result['count']>1:
			self.log("Contact %s has more then one %s email address. Delivering first!" % (query.get('contact_id', 'n/a'), query.get('location_type', 'n/a')),
				logging.WARN, 'pycivi', 'get', 'Email', query.get('contact_id', None), None, time.time()-timestamp)
		elif result['count']==0:
			return None
		return self._createEntity('Email', result['values'][0])


	def createEmail(self, contact_id, location_type_id, email):
		timestamp = time.time()
		query = dict()
		query['action'] 			= 'create'
		query['entity'] 			= 'Email'
		query['location_type_id'] 	= location_type_id
		query['contact_id'] 		= contact_id
		query['email']  			= email
		result = self.performAPICall(query)
		if result['is_error']:
			raise CiviAPIException(result['error_message'])
		return self._createEntity('Email', result['values'][0])


	def getPhoneNumber(self, data):
		timestamp = time.time()
		query = dict()
		query['action'] = 'get'
		query['entity'] = 'Phone'
		query['contact_id'] = data['contact_id']
		query['phone_type'] = data['phone_type']
		query['location_type'] = data['location_type']
		result = self.performAPICall(query)
		if result['is_error']:
			raise CiviAPIException(result['error_message'])
		if result['count']>1:
			self.log("Contact %s has more then one [%s/%s] phone number. Delivering first!" % (query.get('contact_id', 'n/a'), query.get('phone_type', 'n/a'), query.get('location_type', 'n/a')),
				logging.ERROR, 'pycivi', 'get', 'Phone', query.get('contact_id', None), None, time.time()-timestamp)
		elif result['count']==0:
			return None
		return self._createEntity('Phone', result['values'][0])

	def createPhoneNumber(self, data):
		timestamp = time.time()
		query = dict(data)
		query['action'] = 'create'
		query['entity'] = 'Phone'
		result = self.performAPICall(query)
		if result['is_error']:
			raise CiviAPIException(result['error_message'])
		return self._createEntity('Phone', result['values'][0])


	def getOrCreateGreeting(self, greeting_text, postal=False):
		"""
		Looks up or creates the given greeting for postal or email greetign
		"""
		timestamp = time.time()
		if postal:
			option_group = 'email_greeting'
		else:
			option_group = 'postal_greeting'

		option_group_id = self.getOptionGroupID(option_group)
		if not option_group_id:
			self.log("Option group '%s' not found!" % option_group,
				logging.ERROR, 'pycivi', 'getOrCreateGreeting', 'OptionValue', None, None, time.time()-timestamp)
			return

		greeting_id = self.getOptionValueID(option_group_id, greeting_text)
		if greeting_id:
			self.log("Greeting '%s' already exists [%s]" % (greeting_text, greeting_id),
				logging.INFO, 'pycivi', 'getOrCreateGreeting', 'OptionValue', None, None, time.time()-timestamp)
		if not greeting_id:
			greeting_id = self.setOptionValue(option_group_id, greeting_text)
			self.log("Greeting '%s' created [%s]" % (greeting_text, greeting_id),
				logging.INFO, 'pycivi', 'getOrCreateGreeting', 'OptionValue', None, None, time.time()-timestamp)

		return greeting_id


	def getOrCreateTagID(self, tag_name, description = None):
		query = { 'entity': 'Tag',
				  'action': 'get',
				  'name' : tag_name}
		result = self.performAPICall(query)
		if result['count']>1:
			raise CiviAPIException("Tag name query result not unique, this should not happen!")
		elif result['count']==1:
			return result['values'][0]['id']
		else:
			# tag doesn't exist => create
			query['action'] = 'create'
			if description:
				query['description'] = description
			result = self.performAPICall(query)
			return result['values'][0]['id']


	def getContactTagIds(self, entity_id):
		query = { 'entity': 'EntityTag',
				  'contact_id' : entity_id,
				  'action' : 'get',
				  }
		result = self.performAPICall(query)
		if result['is_error']:
			raise CiviAPIException(result['error_message'])
		else:
			count = result['count']
			tags = set()
			for entry in result['values']:
				tags.add(entry['tag_id'])
			if len(tags)!=count:
				raise CiviAPIException("Error: tag count does not match number of delivered tags!")
			return tags



	def tagContact(self, entity_id, tag_id, value=True):
		timestamp = time.time()
		query = { 'entity': 'EntityTag',
				  'contact_id' : entity_id,
				  'tag_id' : tag_id,
				  }
		if value:
			query['action'] = 'create'
		else:
			query['action'] = 'delete'
		result = self.performAPICall(query)
		if result['is_error']:
			raise CiviAPIException(result['error_message'])
		elif result.get('added', False):
			self.log("Added new tag(%s) to contact(%s)" % (tag_id, entity_id),
				logging.INFO, 'pycivi', query['action'], 'EntityTag', entity_id, tag_id, time.time()-timestamp)
		elif result.get('removed', False):
			self.log("Removed tag(%s) from contact(%s)" % (tag_id, entity_id),
				logging.INFO, 'pycivi', query['action'], 'EntityTag', entity_id, tag_id, time.time()-timestamp)
		else:
			self.log("No tags changed for contact#%s" % entity_id,
				logging.DEBUG, 'pycivi', query['action'], 'EntityTag', entity_id, tag_id, time.time()-timestamp)







	def createOrUpdate(self, entity_type, attributes, update_type='update', primary_attributes=[u'id', u'external_identifier']):
		query = dict()
		for key in primary_attributes: 
			if attributes.has_key(key):
				query[key] = attributes[key]
		query['entity'] = entity_type
		query['action'] = 'get'
		result = self.performAPICall(query)
		if result['count']>1:
			raise CiviAPIException("Query result not unique, please provide a unique query for 'getOrCreate'.")
		else:
			if result['count']==1:
				entity = self._createEntity(entity_type, result['values'][0])
				if update_type=='update':
					entity.update(attributes, True)
				elif update_type=='fill':
					entity.fill(attributes, True)
				elif update_type=='replace':
					entity.replace(attributes, True)
				else:
					raise CiviAPIException("Bad update_type '%s' selected. Must be 'update', 'fill' or 'replace'." % update_type)
				return entity
			else:
				query.update(attributes)
				query['action'] = 'create'
				result = self.performAPICall(query)
				if result['is_error']:
					raise CiviAPIException(result['error_message'])
				return self._createEntity(entity_type, result['values'][0])


	def _createEntity(self, entity_type, attributes):
		if entity_type==etype.CONTACT:
			return CiviContactEntity(entity_type, attributes.get('id', None), self, attributes)
		elif entity_type==etype.PHONE:
			return CiviPhoneEntity(entity_type, attributes.get('id', None), self, attributes)
		else:
			return CiviEntity(entity_type, attributes.get('id', None), self, attributes)

