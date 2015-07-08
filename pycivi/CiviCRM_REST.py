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

try:
	import requests
except ImportError as err:
	print "ERROR: Cannot import requests"
	print err
	sys.exit(1)

if LooseVersion(requests.__version__) < LooseVersion('1.1.0'):
	print "ERROR: You need requests >= 1.1.0"
	print "You've got {0}".format(requests.__version__)
	sys.exit(1)



class CiviAPIException(Exception):
	pass

class CiviCRM_REST(CiviCRM):

	def __init__(self, url, site_key, user_key, logfile=None, options=dict()):
		# init some attributes
		CiviCRM.__init__(self, logfile)
		self.url = url
		self.site_key = site_key
		self.user_key = user_key
		self.auth = None
		self.forcePost = False
		self.headers = {}

		if options.has_key('auth_user') and options.has_key('auth_pass'):
			from requests.auth import HTTPBasicAuth
			self.auth = HTTPBasicAuth(options['auth_user'], options['auth_pass'])

		# set rest url
		if self.url.endswith('extern/rest.php'):
			# in this case it's fine, this is the rest URL
			self.rest_url = self.url
		else:
			if self.url.endswith('/civicrm'):
				self.rest_url = self.url[:-8] + '/sites/all/modules/civicrm/extern/rest.php'
			else:
				self.rest_url = self.url + '/sites/all/modules/civicrm/extern/rest.php'


	def performAPICall(self, params=dict(), execParams=dict()):
		timestamp = time.time()
		params['api_key'] = self.user_key
		params['key'] = self.site_key
		params['sequential'] = 1
		params['json'] = 1
		params['version'] = self.api_version
		if self.debug:
			params['debug'] = 1

		forcePost = execParams.get('forcePost', False) or self.forcePost
		if (params['action'] in ['create', 'delete']) or forcePost:
			reply = requests.post(self.rest_url, params=params, verify=False, auth=self.auth, headers=self.headers)
		else:
			reply = requests.get(self.rest_url, params=params, verify=False, auth=self.auth, headers=self.headers)

		self.log("API call completed - status: %d, url: '%s'" % (reply.status_code, reply.url),
			logging.DEBUG, 'API', params.get('action', "NO ACTION SET"), params.get('entity', "NO ENTITY SET!"), params.get('id', ''), params.get('external_identifier', ''), time.time()-timestamp)

		if reply.status_code == 414:
			raise CiviAPIException("Request is too long, please check server settings or use forcePost")
		elif reply.status_code != 200:
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


	def performSimpleAPICall(self, params=dict(), execParams=dict()):
		timestamp = time.time()
		params['api_key'] = self.user_key
		params['key'] = self.site_key
		params['sequential'] = 1
		params['json'] = 1
		params['version'] = self.api_version
		if self.debug:
			params['debug'] = 1

		if (params['action'] in ['create', 'delete']) or (execParams.get('forcePost', False)):
			reply = requests.post(self.rest_url, params=params, verify=False, auth=self.auth)
		else:
			reply = requests.get(self.rest_url, params=params, verify=False, auth=self.auth)

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

		return result
