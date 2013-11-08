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


	def performAPICall(self, params=dict()):
		timestamp = time.time()

		# build call with parameters
		call_params = [self.drush_path, '-r', self.folder, '-l', self.site, 'civicrm-api', '--out=json', '--in=json']
		call_params.append(params['entity'] + '.' + params['action'])
		
		# remove unsuitable parameters
		for non_param in self.non_parameters:
			params.pop(non_param, None)
		query = json.dumps(params)

		try:
			drush = subprocess.Popen(call_params, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.STDOUT)
			reply = drush.communicate(input=query)[0]

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

