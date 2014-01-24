#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
This is a python API wrapper for CiviCRM (https://civicrm.org/)
Copyright (C) 2013 Systopia  (endres@systopia.de)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
'''

__author__      = "Björn Endres"
__copyright__   = "Copyright 2013, Systopia"
__license__     = "GPLv3"
__maintainer__  = "Björn Endres"
__email__       = "endres[at]systopia.de"


import requests
import logging
import sys
import json
import entity_type as etype
import time
import threading
import os
import traceback

from CiviEntity import *

class CiviAPIException(Exception):
	pass

class CiviCRM:

	def __init__(self, url, site_key, user_key, logfile=None):
		raise Exception("You probably meant to call the REST core of the API. Try CiviCRM_REST.CiviCRM_REST(...) instead of CiviCRM.CiviCRM(...)!")

	def __init__(self, logfile=None):
		# init some attributes
		self.lookup_cache = dict()
		self.lookup_cache_lock = threading.Condition()

		# set up logging
		self.logger_format = u"%(level)s;%(type)s;%(entity_type)s;%(first_id)s;%(second_id)s;%(duration)sms;%(thread_id)s;%(text)s"
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
			if not os.path.exists(logfile):
				log = open(logfile, 'w')
				log.write("timestamp;level;module;entity_type;primary_id;secondary_id;execution_time;thread;message\n")
				log.flush()
				log.close()

			logger2 = logging.FileHandler(logfile, mode='a')
			logger2.setLevel(logging.DEBUG)
			logger2.setFormatter(logging.Formatter(u'%(asctime)s;%(message)s'))
			self._logger.addHandler(logger2)
		
		# some more internal attributes
		self.debug = False
		self.api_version = 3
		self._api_calls = 0
		self._api_calls_time = 0.0


	def _getLevelString(self, level):
		"""
		give a textual representation of the log level
		"""
		if level==logging.DEBUG:
			return 'DEBUG'
		elif level==logging.INFO:
			return 'INFO'
		elif level==logging.WARN:
			return 'WARN'
		elif level==logging.ERROR:
			return 'ERROR'
		elif level==logging.FATAL:
			return 'FATAL'
		else:
			return 'UNKNOWN'

	def log(self, message, level=logging.INFO, type='Unknown', command='Unknown', entity_type='', first_id='', second_id='', duration='0'):
		"""
		formally log information.
		"""
		try:
			duration = str(int(duration * 1000))
		except:
			duration = '0'

		self._logger.log(level, self.logger_format,  { 	'type': type,
														'level': self._getLevelString(level),
														'command': command,
														'entity_type': entity_type,
														'first_id': first_id,
														'second_id': second_id,
														'duration': duration,
														'thread_id': threading.currentThread().name,
														'text': message,
													})

	def logException(self, message="An exception occurred: ", level=logging.ERROR, type='Unknown', command='Unknown', entity_type='', first_id='', second_id='', duration='0'):
		"""
		log current exception (in except: block)
		"""
		exception_text = ' >> ' + traceback.format_exc() + ' <<'
		exception_text = exception_text.replace('\x0A', '  ||')
		self.log(message + exception_text, level, type, command, entity_type, first_id, second_id, duration)


	def performAPICall(self, params=dict()):
		raise NotImplementedError("You need to use a CiviCRM implementation like CiviCRM_DRUSH or CiviCRM_REST!")


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

	

	def getEntity(self, entity_type, attributes, primary_attributes=['id','external_identifier']):
		timestamp = time.time()

		query = dict()
		first_key = None
		for key in primary_attributes: 
			if attributes.has_key(key):
				query[key] = attributes[key]
				if first_key==None:
					first_key = attributes[key]
		if not len(query) > 0:
			self.log("No primary key provided with contact '%s'." % str(attributes),
				logging.DEBUG, 'pycivi', 'get', entity_type, first_key, None, time.time()-timestamp)
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


	def getEntities(self, entity_type, attributes, primary_attributes=['id','external_identifier']):
		timestamp = time.time()

		query = dict()
		first_key = None
		for key in primary_attributes: 
			if attributes.has_key(key):
				query[key] = attributes[key]
				if first_key==None:
					first_key = attributes[key]
		if not len(query) > 0:
			self.log("No primary key provided with the following specs: '%s'." % str(attributes),
				logging.WARN, 'pycivi', 'get', entity_type, first_key, None, time.time()-timestamp)
			return []

		query['entity'] = entity_type
		query['action'] = 'get'
		result = self.performAPICall(query)
		if result['is_error']:
			raise CiviAPIException(result['error_message'])

		entities = list()
		self.log("Entities found: %s" % result['count'],
			logging.DEBUG, 'pycivi', 'get', entity_type, first_key, None, time.time()-timestamp)
		for entity_data in result['values']:
			entity = self._createEntity(entity_type, entity_data)
			entities.append(entity)
		return entities


	def createEntity(self, entity_type, attributes):
		"""
		simply creates a new entity of the given type
		"""
		timestamp = time.time()
		query = dict(attributes)
		query['action'] 			= 'create'
		query['entity'] 			= entity_type
		result = self.performAPICall(query)
		if result['is_error']:
			raise CiviAPIException(result['error_message'])
		return self._createEntity(entity_type, result['values'][0])


	def createOrUpdate(self, entity_type, attributes, update_type='update', primary_attributes=[u'id', u'external_identifier']):
		query = dict()
		for key in primary_attributes: 
			if attributes.has_key(key):
				query[key] = attributes[key]

		if query:
			# try to find the entity
			query['entity'] = entity_type
			query['action'] = 'get'
			result = self.performAPICall(query)
		else:
			# if there are no criteria given, not results should be expected
			result = {'count': 0}

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
				query['entity'] = entity_type
				query['action'] = 'create'
				result = self.performAPICall(query)
				if result['is_error']:
					raise CiviAPIException(result['error_message'])
				if type(result['values'])==dict:
					return self._createEntity(entity_type, result['values'][str(result['id'])])
				else:
					return self._createEntity(entity_type, result['values'][0])	


	###########################################################################
	#                            Lookup methods                               #
	###########################################################################


	def getContactID(self, attributes, primary_attributes=['external_identifier'], search_deleted=True):
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
			if search_deleted and not int(attributes.get('is_deleted', '0'))==1:
				# NOT found, but we haven't looked into the deleted contacts
				#print "NOT FOUND. LOOKING IN DELTED."
				new_attributes = dict(attributes)
				new_primary_attributes = list(primary_attributes)
				new_attributes['is_deleted'] = '1'
				new_primary_attributes += ['is_deleted']
				return self.getContactID(new_attributes, new_primary_attributes, search_deleted)
			#print "STILL NOT FOUND!"
			self.log("Contact not found.",
				logging.DEBUG, 'pycivi', 'get', 'Contact', first_key, None, time.time()-timestamp)
			return 0

	
	def getEntityID(self, attributes, entity_type, primary_attributes):
		timestamp = time.time()
		if attributes.has_key('id'):
			return attributes['id']
		
		query = dict()
		first_key = None
		for key in primary_attributes: 
			if attributes.has_key(key):
				query[key] = attributes[key]
				if first_key==None:
					first_key = attributes[key]
		if not len(query) > 0:
			self.log("No primary key provided with entity '%s'." % str(attributes),
				logging.DEBUG, 'pycivi', 'get', 'Entity', first_key, None, time.time()-timestamp)
			return 0

		query['entity'] = entity_type
		query['action'] = 'get'
		query['return'] = 'id'

		result = self.performAPICall(query)
		if result['count']>1:
			self.log("Query result not unique, please provide a unique query for 'getOrCreate'.",
				logging.WARN, 'pycivi', 'get', 'Entity', first_key, None, time.time()-timestamp)
			raise CiviAPIException("Query result not unique, please provide a unique query for 'getOrCreate'.")
		elif result['count']==1:
			entity_id = result['values'][0]['id']
			self.log("Entity ID resolved.",
				logging.DEBUG, 'pycivi', 'get', 'Entity', first_key, None, time.time()-timestamp)
			return entity_id
		self.log("Entity not found.",
			logging.DEBUG, 'pycivi', 'get', 'Entity', first_key, None, time.time()-timestamp)
		return 0

	def getCampaignID(self, attribute_value, attribute_key='title'):
		"""
		Get the ID for a given campaign

		Results will be cached
		"""
		timestamp = time.time()
		if self.lookup_cache.has_key('campaign') and self.lookup_cache['campaign'].has_key(attribute_key) and self.lookup_cache['campaign'][attribute_key].has_key(attribute_value):
			return self.lookup_cache['campaign'][attribute_key][attribute_value]

		query = dict()
		query['entity'] = 'Campaign'
		query['action'] = 'get'
		query[attribute_key] = attribute_value
		result = self.performAPICall(query)
		if result['count']>1:
			campaign_id = 0
			self.log(u"More than one campaign found with %s '%s'!" % (attribute_key, attribute_value),
				logging.WARN, 'pycivi', 'getCampaignID', 'Campaign', None, None, time.time()-timestamp)
		elif result['count']==0:
			campaign_id = 0
			self.log(u"No campaign found with %s '%s'!" % (attribute_key, attribute_value),
				logging.DEBUG, 'pycivi', 'getCampaignID', 'Campaign', None, None, time.time()-timestamp)
		else:
			campaign_id = result['values'][0]['id']
			self.log(u"Campaign with %s '%s' resolved to ID %s!" % (attribute_key, attribute_value, campaign_id),
				logging.DEBUG, 'pycivi', 'getCampaignID', 'Campaign', None, None, time.time()-timestamp)

		# store value
		self.lookup_cache_lock.acquire()
		if not self.lookup_cache.has_key('campaign'):
			self.lookup_cache['campaign'] = dict()
		if not self.lookup_cache['campaign'].has_key(attribute_key):
			self.lookup_cache['campaign'][attribute_key] = dict()
		self.lookup_cache['campaign'][attribute_key][attribute_value] = campaign_id
		self.lookup_cache_lock.notifyAll()
		self.lookup_cache_lock.release()

		return campaign_id


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
		if result['count']>1:
			field_id = 0
			self.log(u"More than one custom field found with name '%s'!" % field_name,
				logging.WARN, 'API', 'get', 'CustomField', None, None, time.time()-timestamp)
		elif result['count']==0:
			field_id = 0
			self.log(u"Custom field '%s' does not exist." % field_name,
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


	def setCustomFieldOptionValue(self, entity_id, field_name, value, entity_type='Contact', create_option_value_if_not_exists=True):
		"""
		Sets a custom field's option value

		Option Value custom fields (values selected from predefined set) are special,
		 we have to look up the custom field, and then the option group values.
		"""

		timestamp = time.time()

		#value should not be an empty string
		if not value:
			self.log(u"OptionValue not set for field %s." % field_name,
				logging.WARN, 'API', 'get', 'CustomField', None, None, time.time()-timestamp)
			return

		field_id = self.getCustomFieldID(field_name)
		if not field_id:
			self.log(u"Custom field '%s' does not exist." % field_name,
				logging.WARN, 'API', 'get', 'CustomField', None, None, time.time()-timestamp)
			return

		# get the associated option group id
		if self.lookup_cache.has_key('custom_field_optiongroup') and self.lookup_cache['custom_field_optiongroup'].has_key(field_name):
			option_group_id = self.lookup_cache['custom_field_optiongroup'][field_name]
		else:
			query = dict()
			query['entity'] = 'CustomField'
			query['action'] = 'get'
			query['id'] = field_id

			result = self.performAPICall(query)
			option_group_id = 0
			if result['is_error']:
				raise CiviAPIException(result['error_message'])
			if result['count']>1:
				self.log(u"More than one custom field found with name '%s'!" % field_name,
					logging.WARN, 'API', 'get', 'CustomField', None, None, time.time()-timestamp)
			elif result['count']==0:
				self.log(u"Custom field '%s' does not exist." % field_name,
					logging.WARN, 'API', 'get', 'CustomField', None, None, time.time()-timestamp)
			elif not result['values'][0].has_key('option_group_id'):
				self.log(u"Custom field '%s' is not a option_value type field." % field_name,
					logging.WARN, 'API', 'get', 'CustomField', None, None, time.time()-timestamp)
			else:
				option_group_id = result['values'][0]['option_group_id']
				self.log(u"Custom field '%s' resolved to ID %s" % (field_name, field_id),
					logging.DEBUG, 'API', 'get', 'CustomField', field_id, None, time.time()-timestamp)

			# store value
			self.lookup_cache_lock.acquire()
			if not self.lookup_cache.has_key('custom_field_optiongroup'):
				self.lookup_cache['custom_field_optiongroup'] = dict()
			self.lookup_cache['custom_field_optiongroup'][field_name] = option_group_id
			self.lookup_cache_lock.notifyAll()
			self.lookup_cache_lock.release()

		if not option_group_id:
			self.log(u"Custom field '%s' cannot be set. Either not found or not a custom_value type." % field_name,
				logging.WARN, 'API', 'get', 'CustomField', None, None, time.time()-timestamp)
			return

		# now that we have the option_group_id, check, if it's already there
		option_value_id = self.getOptionValueID(option_group_id, value)
		if (not option_value_id) and create_option_value_if_not_exists:
			# this option value does not exist yet => create!
			option_value_id = self.setOptionValue(option_group_id, value, attributes={'value': value, 'label': value})

		if not option_value_id:
			self.log(u"Custom field '%s' cannot be set. There is no predefined option for value '%s'" % (field_name, value),
				logging.WARN, 'API', 'get', 'CustomField', None, None, time.time()-timestamp)
			return

		# now we have option_group_id and option_value_id, we can *finally* set the custom field
		self.setCustomFieldValue(entity_id, field_name, value, entity_type)


	def setCustomFieldValue(self, entity_id, field_name, value, entity_type='Contact'):
		"""
		Sets a custom field's value
		"""
		timestamp = time.time()
		field_id = self.getCustomFieldID(field_name, entity_type)
		if not field_id:
			self.log(u"Custom field '%s' does not exist." % field_name,
				logging.WARN, 'API', 'get', 'CustomField', None, None, time.time()-timestamp)
			return

		# now we have option_group_id and option_value_id, we can *finally* set the custom field
		query = dict()
		query['entity'] = 'CustomValue'
		query['action'] = 'create'
		query['entity_id'] = entity_id
		query['custom_%s' % field_id] = value
		result = self.performAPICall(query)
		if result['is_error']:
			raise CiviAPIException(result['error_message'])
		self.log(u"Set custom field '%s' to value '%s'" % (field_name, value),
			logging.DEBUG, 'API', 'get', 'CustomField', field_id, None, time.time()-timestamp)
		return


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
		if self.lookup_cache.has_key('option_value_id') and self.lookup_cache['option_value_id'].has_key(option_group_id) and self.lookup_cache['option_value_id'][option_group_id].has_key(name):
			return self.lookup_cache['option_value_id'][option_group_id][name]

		query = dict()
		query['entity'] = 'OptionValue'
		query['action'] = 'get'
		query['name'] = name
		query['option_group_id'] = option_group_id
		result = self.performAPICall(query)
		if result['is_error']:
			raise CiviAPIException(result['error_message'])
		if result['count']>1:
			value_id = result['values'][0]['id']
			self.log("More than one value found with name '%s'! Using first one..." % name,
				logging.WARN, 'API', 'get', 'OptionValue', None, None, time.time()-timestamp)
		elif result['count']==0:
			value_id = 0
			self.log("Value '%s' does not exist." % name,
				logging.DEBUG, 'API', 'get', 'OptionValue', value_id, None, time.time()-timestamp)
		else:
			#value_id = result['values'][0]['value']
			value_id = result['values'][0]['id']
			self.log("Value '%s' resolved to ID %s" % (name, value_id),
				logging.DEBUG, 'API', 'get', 'OptionValue', value_id, None, time.time()-timestamp)

		# store value
		if value_id:
			self.lookup_cache_lock.acquire()
			if not self.lookup_cache.has_key('option_value_id'):
				self.lookup_cache['option_value_id'] = dict()
			if not self.lookup_cache['option_value_id'].has_key(option_group_id):
				self.lookup_cache['option_value_id'][option_group_id] = dict()
			self.lookup_cache['option_value_id'][option_group_id][name] = value_id
			self.lookup_cache_lock.notifyAll()
			self.lookup_cache_lock.release()

		return value_id


	def getOptionValue(self, option_group_id, name):
		"""
		Get the 'value' for a given option value
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
			value = 0
			self.log("More than one value found with name '%s'!" % name,
				logging.WARN, 'API', 'get', 'OptionValue', None, None, time.time()-timestamp)
		elif result['count']==0:
			value = 0
			self.log("Value '%s' does not exist." % name,
				logging.DEBUG, 'API', 'get', 'OptionValue', value, None, time.time()-timestamp)
		else:
			value = result['values'][0]['value']
			self.log("Value '%s' resolved to ID %s" % (name, value),
				logging.DEBUG, 'API', 'get', 'OptionValue', value, None, time.time()-timestamp)

		# store value
		self.lookup_cache_lock.acquire()
		if not self.lookup_cache.has_key('option_value'):
			self.lookup_cache['option_value'] = dict()
		if not self.lookup_cache['option_value'].has_key(option_group_id):
			self.lookup_cache['option_value'][option_group_id] = dict()			
		self.lookup_cache['option_value'][option_group_id][name] = value
		self.lookup_cache_lock.notifyAll()
		self.lookup_cache_lock.release()

		return value
		

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
		value_id = result['values'][0]['value']
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


	def getMembershipStatusID(self, membership_status_name):
		# first: look up in cache
		if self.lookup_cache.has_key('membership_status2id') and self.lookup_cache['membership_status2id'].has_key(membership_status_name):
			return self.lookup_cache['membership_status2id'][membership_status_name]

		timestamp = time.time()
		query = { 	'action': 'get',
					'entity': 'MembershipStatus',
					'name': membership_status_name }
		result = self.performAPICall(query)
		if result['count']>1:
			self.log("Non-uniqe membership status name '%s'" % membership_status_name,
				logging.WARN, 'API', 'get', 'MembershipStatus', None, None, time.time()-timestamp)
			raise CiviAPIException("Non-uniqe membership status name '%s'" % membership_status_name)
		elif result['count']==1:
			status_id = result['values'][0]['id']
			self.log("Membership status '%s' resolved to id %s." % (membership_status_name, status_id),
				logging.DEBUG, 'API', 'get', 'MembershipStatus', status_id, None, time.time()-timestamp)
		else:
			status_id = 0
			self.log("Membership status '%s' could NOT be resolved",
				logging.DEBUG, 'API', 'get', 'MembershipStatus', None, None, time.time()-timestamp)

		self.lookup_cache_lock.acquire()
		if not self.lookup_cache.has_key('membership_status2id'):
			self.lookup_cache['membership_status2id'] = dict()
		self.lookup_cache['membership_status2id'][membership_status_name] = status_id
		self.lookup_cache_lock.notifyAll()
		self.lookup_cache_lock.release()
		return status_id



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


	def getEmails(self, contact_id, location_type_id=None):
		timestamp = time.time()
		query = dict()
		query['action']			 	= 'get'
		query['entity'] 			= 'Email'
		query['contact_id'] 		= contact_id
		if location_type_id:
			query['location_type_id'] 	= location_type_id

		result = self.performAPICall(query)
		if result['is_error']:
			raise CiviAPIException(result['error_message'])

		emails = list()
		for email_data in result['values']:
			emails.append(self._createEntity('Email', email_data))

		self.log("Found %d email addresses (type %s) for contact %s." % (len(emails), location_type_id, query.get('contact_id', 'n/a')),
			logging.DEBUG, 'pycivi', 'get', 'Email', query.get('contact_id', None), None, time.time()-timestamp)

		return emails


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

	def getPhoneNumbers(self, contact_id, location_type_id=None):
		timestamp = time.time()
		query = dict()
		query['action']			 	= 'get'
		query['entity'] 			= 'Phone'
		query['contact_id'] 		= contact_id
		if location_type_id:
			query['location_type_id'] 	= location_type_id

		result = self.performAPICall(query)
		if result['is_error']:
			raise CiviAPIException(result['error_message'])

		phones = list()
		for phone_data in result['values']:
			phones.append(self._createEntity('Phone', phone_data))

		self.log("Found %d phone numbers (type %s) for contact %s." % (len(phones), location_type_id, query.get('contact_id', 'n/a')),
			logging.DEBUG, 'pycivi', 'get', 'Phone', query.get('contact_id', None), None, time.time()-timestamp)

		return phones

	def createPhoneNumber(self, data):
		timestamp = time.time()
		query = dict(data)
		query['action'] = 'create'
		query['entity'] = 'Phone'
		result = self.performAPICall(query)
		if result['is_error']:
			raise CiviAPIException(result['error_message'])
		return self._createEntity('Phone', result['values'][0])


	def getWebsites(self, contact_id, website_type_id=None):
		timestamp = time.time()
		query = dict()
		query['action']			 	= 'get'
		query['entity'] 			= 'Website'
		query['contact_id'] 		= contact_id
		if website_type_id:
			query['website_type_id'] 	= website_type_id

		result = self.performAPICall(query)
		if result['is_error']:
			raise CiviAPIException(result['error_message'])

		sites = list()
		for site_data in result['values']:
			sites.append(self._createEntity('Website', site_data))

		self.log("Found %d websites (type %s) for contact %s." % (len(sites), website_type_id, query.get('contact_id', 'n/a')),
			logging.DEBUG, 'pycivi', 'get', 'Website', query.get('contact_id', None), None, time.time()-timestamp)

		return sites

	def createWebsite(self, data):
		timestamp = time.time()
		query = dict(data)
		query['action'] = 'create'
		query['entity'] = 'Website'
		result = self.performAPICall(query)
		if result['is_error']:
			raise CiviAPIException(result['error_message'])
		return self._createEntity('Website', result['values'][0])

	def getOrCreatePrefix(self, prefix_text):
		"""
		Looks up or creates the given individual prefix
		"""
		timestamp = time.time()
		if not prefix_text:
			self.log("Will not create empty prefix (default)",
				logging.WARN, 'pycivi', 'getOrCreatePrefix', 'OptionValue', None, None, time.time()-timestamp)
			return

		option_group = 'individual_prefix'
		option_group_id = self.getOptionGroupID(option_group)
		if not option_group_id:
			self.log("Option group '%s' not found!" % option_group,
				logging.ERROR, 'pycivi', 'getOrCreatePrefix', 'OptionGroup', None, None, time.time()-timestamp)
			return

		greeting_id = self.getOptionValue(option_group_id, prefix_text)
		if greeting_id:
			self.log("Prefix '%s' already exists [%s]" % (prefix_text, greeting_id),
				logging.INFO, 'pycivi', 'getOrCreatePrefix', 'OptionValue', None, None, time.time()-timestamp)
		if not greeting_id:
			greeting_id = self.setOptionValue(option_group_id, prefix_text)
			self.log("Prefix '%s' created [%s]" % (prefix_text, greeting_id),
				logging.INFO, 'pycivi', 'getOrCreatePrefix', 'OptionValue', None, None, time.time()-timestamp)

		return greeting_id


	def getOrCreateGreeting(self, greeting_text, postal=False):
		"""
		Looks up or creates the given greeting for postal or email greetign
		"""
		timestamp = time.time()
		if postal:
			option_group = 'postal_greeting'
		else:
			option_group = 'email_greeting'

		option_group_id = self.getOptionGroupID(option_group)
		if not option_group_id:
			self.log("Option group '%s' not found!" % option_group,
				logging.ERROR, 'pycivi', 'getOrCreateGreeting', 'OptionGroup', None, None, time.time()-timestamp)
			return

		greeting_id = self.getOptionValue(option_group_id, greeting_text)
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


	def getOrCreateGroupID(self, group_name, description = None):
		query = { 'entity': 'Group',
				  'action': 'get',
				  'title' : group_name}
		result = self.performAPICall(query)
		if result['count']>1:
			raise CiviAPIException("Group name query result not unique, this should not happen!")
		elif result['count']==1:
			return result['values'][0]['id']
		else:
			# group doesn't exist => create
			query['action'] = 'create'
			query['group_type'] = '[2]'  # set as Mailing Group
			if description:
				query['description'] = description
			result = self.performAPICall(query)
			return result['values'][0]['id']


	def getContactTagIds(self, entity_id):
		# TODO: can it be safely replace by
		#    return self.getEntityTagIds(entity_id, 'civicrm_contact')
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


	def getEntityTagIds(self, entity_id, entity_table):
		query = { 'entity': 		'EntityTag',
				  'entity_id' : 	entity_id,
				  'entity_table' : 	entity_table,
				  'action' : 		'get',
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


	def getContactGroupIds(self, entity_id):
		query = { 'entity': 'GroupContact',
				  'contact_id' : entity_id,
				  'action' : 'get',
				  }
		result = self.performAPICall(query)
		count = result['count']
		groups = set()
		for entry in result['values']:
			groups.add(entry['group_id'])
		if len(groups)!=count:
			raise CiviAPIException("Error: group count does not match number of delivered group!")
		return groups


	def tagContact(self, entity_id, tag_id, value=True):
		# TODO: can it safely be replaced by
		#	self.tagEntity(entity_id, 'cvicirm_contact', tag_id, value)
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


	def tagEntity(self, entity_id, entity_table, tag_id, value=True):
		timestamp = time.time()
		query = { 'entity': 		'EntityTag',
				  'entity_id': 		entity_id,
				  'entity_table': 	entity_table,
				  'tag_id': 		tag_id,
				  }
		if value:
			query['action'] = 'create'
		else:
			query['action'] = 'delete'
		result = self.performAPICall(query)
		if result['is_error']:
			raise CiviAPIException(result['error_message'])
		elif result.get('added', False):
			self.log("Added new tag(%s) to entity #%s" % (tag_id, entity_id),
				logging.INFO, 'pycivi', query['action'], 'EntityTag', entity_id, tag_id, time.time()-timestamp)
		elif result.get('removed', False):
			self.log("Removed tag(%s) from entity #%s" % (tag_id, entity_id),
				logging.INFO, 'pycivi', query['action'], 'EntityTag', entity_id, tag_id, time.time()-timestamp)
		else:
			self.log("No tags changed for entity #%s" % entity_id,
				logging.DEBUG, 'pycivi', query['action'], 'EntityTag', entity_id, tag_id, time.time()-timestamp)


	def setGroupMembership(self, entity_id, group_id, value=True):
		timestamp = time.time()
		query = { 'entity': 'GroupContact',
				  'contact_id' : entity_id,
				  'group_id' : group_id,
				  }
		if value:
			query['action'] = 'create'
		else:
			query['action'] = 'delete'
		result = self.performAPICall(query)
		if result['is_error']:
			raise CiviAPIException(result['error_message'])
		elif result.get('added', False):
			self.log("Added contact (%s) to group (%s)." % (entity_id, group_id),
				logging.INFO, 'pycivi', query['action'], 'GroupContact', entity_id, group_id, time.time()-timestamp)
		elif result.get('removed', False):
			self.log("Removed contact (%s) from group (%s)." % (entity_id, group_id),
				logging.INFO, 'pycivi', query['action'], 'GroupContact', entity_id, group_id, time.time()-timestamp)
		else:
			self.log("No group membership changed for contact (%s)" % entity_id,
				logging.DEBUG, 'pycivi', query['action'], 'GroupContact', entity_id, group_id, time.time()-timestamp)




	def _createEntity(self, entity_type, attributes):
		if entity_type==etype.CONTACT:
			return CiviContactEntity(entity_type, attributes.get('id', None), self, attributes)
		elif entity_type==etype.CONTRIBUTION:
			return CiviContributionEntity(entity_type, attributes.get('id', None), self, attributes)
		elif entity_type==etype.PHONE:
			return CiviPhoneEntity(entity_type, attributes.get('id', None), self, attributes)
		elif entity_type==etype.CAMPAIGN:
			return CiviCampaignEntity(entity_type, attributes.get('id', None), self, attributes)
		elif entity_type==etype.NOTE:
			return CiviNoteEntity(entity_type, attributes.get('id', None), self, attributes)
		elif entity_type==etype.RELATIONSHIP_TYPE:
			return CiviRelationshipTypeEntity(entity_type, attributes.get('id', None), self, attributes)
		elif entity_type==etype.ADDRESS:
			return CiviAddressEntity(entity_type, attributes.get('id', None), self, attributes)
		elif entity_type==etype.EMAIL:
			return CiviEmailEntity(entity_type, attributes.get('id', None), self, attributes)
		else:
			return CiviEntity(entity_type, attributes.get('id', None), self, attributes)

