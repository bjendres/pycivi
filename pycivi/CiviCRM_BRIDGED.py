#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
This is a python API wrapper for CiviCRM (https://civicrm.org/)
Copyright (C) 2016 Systopia  (endres@systopia.de)

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
__copyright__   = "Copyright 2016, Systopia"
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
import random
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

class CiviCRM_BRIDGED(CiviCRM):


	def __init__(self, instance, logfile=None, options=dict()):
		CiviCRM.__init__(self, logfile)
		self.wrapped_instance = instance
		self.bridge = None
		self.calls = dict()
		self.call_base    = random.randint(1000000,9999999)
		self.call_counter = 1

		self.auth = None
		self.headers = {}
		if options.has_key('auth_user') and options.has_key('auth_pass'):
			from requests.auth import HTTPBasicAuth
			self.auth = HTTPBasicAuth(options['auth_user'], options['auth_pass'])


	def performAPICall(self, params=dict(), execParams=dict()):
		callID = self.queueCall(params)
		return self.fetchCall(callID)

	
	def probe(self):
		bridge = self.getBridge()
		return bridge != None


	def fetchCall(self, call_id):
		bridge = self.getBridge()
		if bridge:
			url = bridge['fetch_url'] + '&call_id=' + call_id
			reply = requests.get(url, verify=False, auth=self.auth, headers=self.headers)
			result = json.loads(reply.text)
			return result



	def queueCall(self, call_data):
		bridge = self.getBridge()
		if bridge:
			call_id = '%s-%06d' % tuple([self.call_base, self.call_counter])
			self.call_counter += 1
			url = bridge['push_url'] + '&call_id=' + call_id
			requests.post(url, data=json.dumps(call_data), verify=False, auth=self.auth, headers=self.headers)
			return call_id
		



	def getBridge(self):
		if self.bridge:
			return self.bridge

		result = self.wrapped_instance.performAPICall({'action': 'create', 'entity': 'ApiBridge'})
		bridge = result['values']
		if bridge.get('bridge_key', None):
			self.bridge = bridge
			return bridge
		else:
			return
