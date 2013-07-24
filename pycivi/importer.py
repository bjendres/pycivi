import CiviCRM, entity_type
import csv
import codecs
import threading
import logging
import time

class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")

class UnicodeReader:
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        f = UTF8Recoder(f, encoding)
        self.reader = csv.reader(f, dialect=dialect, **kwds)

    def next(self):
        row = self.reader.next()
        return [unicode(s, "utf-8") for s in row]

    def __iter__(self):
        return self


class CSVRecordSource:
	def __init__(self, csv_file, mapping=dict(), transformations=dict(), delimiter=','):
		inputStream = open(csv_file, 'rb')
		self.reader = UnicodeReader(inputStream, 'excel', 'utf8', delimiter=delimiter, quotechar='"')
		self.mapping = mapping
		self.transformations = transformations
		self.row_iterator = None
		self.header = None


	def __iter__(self):
		# I know this is a dirty hack... sorry about that
		self.row_iterator = self.reader.__iter__()
		self.header = self.row_iterator.next()
		return self

	def next(self):
		if self.row_iterator:
			row = self.row_iterator.next()
			
			# build record
			record = dict()
			for i in range(len(row)):
				field = self.header[i]
				data = row[i]
				if field in self.mapping:
					field = self.mapping[field]
				if field!=None:
					# see if there is a translation
					if field in self.transformations:
						#print "Replace %s with %s." % (data, self.transformations[field].get(data, data))
						data = self.transformations[field].get(data, data)
					record[field] = data
			return record


def _get_or_create_from_params(name, parameters, create_entry=dict()):
	entry = parameters.get(name, None)
	if entry==None:	
		parameters_lock = parameters['lock']
		parameters_lock.acquire()
		# test again, maybe another thread already created them...
		entry = parameters.get(name, None)
		if entry==None:
			# no? ok, then it's up to us to query the tag ids
			entry = create_entry
			parameters[name] = entry
		parameters_lock.notifyAll()
		parameters_lock.release()
	return entry

def _prepare_parameters(parameters):
	if not parameters.has_key('lock'):
		parameters['lock'] = threading.Condition()


def import_contributions(civicrm, record_source, parameters=dict()):
	"""
	Imports import_contributions

	parameters['update_mode'] can be set to anything CiviCRM.createOrUpdate accepts
	parameters['id'] can be set to the identifying field (e.g. 'external_identifier' or 'trxn_id')
	"""
	_prepare_parameters(parameters)
	timestamp = time.time()
	entity_type = parameters.get('entity_type', 'Contribution')
	update_mode = parameters.get('update_mode', 'update')
	for record in record_source:
		update = dict(record)
		# lookup contact_id
		if update.has_key('contact_external_identifier'):
			if update['contact_external_identifier']:
				update['contact_id'] = civicrm.getContactID({'external_identifier': update['contact_external_identifier']})
			del update['contact_external_identifier']
		if not update.has_key('contact_id') or not update['contact_id']:
			civicrm.log(u"Contact not found! No valid contact reference specified in (%s)" % unicode(str(record), 'utf8'),
				logging.ERROR, 'importer', 'import_contributions', 'Contribution', None, None, time.time()-timestamp)
			continue
		
		# lookup payment type
		if update.has_key('payment_instrument'):
			if update['payment_instrument']:
				update['payment_instrument_id'] = civicrm.getOptionValueID(civicrm.getOptionGroupID('payment_instrument'), update['payment_instrument'])
			del update['payment_instrument']
		if not update.has_key('payment_instrument_id') or not update['payment_instrument_id']:
			civicrm.log(u"Payment type ID not found! No valid payment type specified in (%s)" % unicode(str(record), 'utf8'),
				logging.ERROR, 'importer', 'import_contributions', 'Contribution', None, None, time.time()-timestamp)
			continue

		# lookup campaign
		if update.has_key('contribution_campaign'):
			if update['contribution_campaign']:
				update['contribution_campaign_id'] = civicrm.getCampaignID(update['contribution_campaign'])
				if not update['contribution_campaign_id']:
					civicrm.log(u"Campaign ID not found! No valid campaign specified in (%s)" % unicode(str(record), 'utf8'),
						logging.WARN, 'importer', 'import_contributions', 'Contribution', None, None, time.time()-timestamp)
			del update['contribution_campaign']

		# lookup contribution status
		if update.has_key('contribution_status'):
			if update['contribution_status']:
				update['contribution_status_id'] = civicrm.getOptionValueID(civicrm.getOptionGroupID('contribution_status'), update['contribution_status'])
			del update['contribution_status']
		if not update.has_key('contribution_status_id') or not update['contribution_status_id']:
			civicrm.log(u"Contribution status ID not found! No valid contribution status specified in (%s)" % unicode(str(record), 'utf8'),
				logging.ERROR, 'importer', 'import_contributions', 'Contribution', None, None, time.time()-timestamp)
			continue

		entity = civicrm.createOrUpdate(entity_type, update, update_mode, ['contribution_id', 'trxn_id'])
		civicrm.log(u"Wrote contribution '%s'" % unicode(str(entity), 'utf8'),
			logging.INFO, 'importer', 'import_contributions', 'Contribution', entity.get('id'), None, time.time()-timestamp)


def import_campaigns(civicrm, record_source, parameters=dict()):
	"""
	Imports campaigns

	parameters['update_mode'] can be set to anything CiviCRM.createOrUpdate accepts
	parameters['id'] can be set to the identifying field (e.g. 'external_identifier' or 'name')
	"""
	_prepare_parameters(parameters)
	timestamp = time.time()
	entity_type = parameters.get('entity_type', 'Campaign')
	update_mode = parameters.get('update_mode', 'update')
	for record in record_source:
		
		update = dict(record)
		# lookup campaign type
		if update.has_key('campaign_type'):
			if update['campaign_type']:
				update['campaign_type_id'] = civicrm.getOptionValueID(civicrm.getOptionGroupID('campaign_type'), update['campaign_type'])
			del update['campaign_type']
		if not update.has_key('campaign_type_id') or not update['campaign_type_id']:
			civicrm.log(u"Campaign type ID not identified! No valid campaign type specified in (%s)" % unicode(str(record), 'utf8'),
				logging.ERROR, 'importer', 'import_campaigns', 'Campaign', None, None, time.time()-timestamp)
			continue
		
		# lookup campaign status
		if update.has_key('status'):
			if update['status']:
				update['status_id'] = civicrm.getOptionValueID(civicrm.getOptionGroupID('campaign_status'), update['status'])
			del update['status']
		if not update.has_key('campaign_type_id') or not update['campaign_type_id']:
			civicrm.log(u"Campaign status ID not identified! No valid campaign status specified in (%s)" % unicode(str(record), 'utf8'),
				logging.ERROR, 'importer', 'import_campaigns', 'Campaign', None, None, time.time()-timestamp)
			continue

		if parameters.has_key('id'):
			entity = civicrm.createOrUpdate(entity_type, update, update_mode, [parameters['id']])
		else:
			entity = civicrm.createOrUpdate(entity_type, update, update_mode)
		civicrm.log(u"Wrote campaign '%s'" % unicode(str(entity), 'utf8'),
			logging.INFO, 'importer', 'import_campaign', 'Campaign', entity.get('id'), None, time.time()-timestamp)



def import_contact_address(civicrm, record_source, parameters=dict()):
	"""
	Makes sure, that the contact has the given address

	Possible record entries:
	"id", "contact_id", "location_type_id", "is_primary", "is_billing", "street_address", 
	"supplemental_address_1", "supplemental_address_2", "city", "postal_code", "country_id", 
	"manual_geo_code", 

	Parameters:
	parameters['location_type'] 		sets the type (default "Main") if no information provided by record
	parameters['update_mode'] 			either set to "update", "fill" or "replace" - or "add" to create a new entry
	"""
	_prepare_parameters(parameters)
	for record in record_source:
		timestamp = time.time()
		record['contact_id'] = civicrm.getContactID(record)
		if not record['contact_id']:
			civicrm.log(u"Could not write contact address, contact not found for '%s'" % str(record),
				logging.WARN, 'importer', 'import_contact_address', 'Address', None, None, time.time()-timestamp)
			continue

		# get the location type id
		if (not record.has_key('location_type_id')):
			location_type = record.get('location_type', parameters.get('location_type', 'Main'))
			location_type_dict = _get_or_create_from_params('location_type_dict', parameters)

			if location_type_dict.has_key(location_type):
				record['location_type_id'] = location_type_dict[location_type]
			else:
				record['location_type_id'] = civicrm.getLocationTypeID(location_type)
				location_type_dict[location_type] = record['location_type_id']

		mode = parameters.get('update_mode', 'update')
		if mode in ['update', 'fill', 'replace']:
			try:
				address = civicrm.createOrUpdate('Address', record, update_type=mode, primary_attributes=['contact_id', 'location_type_id'])
				civicrm.log(u"Wrote contact address for '%s'" % unicode(str(address), 'utf8'),
					logging.INFO, 'importer', 'import_contact_address', 'Address', address.get('id'), None, time.time()-timestamp)
			except:
				civicrm.logException("Exception while importing address for [%s]. Data was %s, exception: " % (record['contact_id'], str(record)),
					logging.ERROR, 'importer', 'import_contact_address', 'Address', None, record['contact_id'], time.time()-timestamp)
		else:
			civicrm.log("Update mode '%s' not implemented!" % mode,
				logging.ERROR, 'importer', 'import_contact_address', 'Address', None, record['contact_id'], time.time()-timestamp)
			#entity_type = parameters.get('entity_type', 'Contact')
			#update_mode = parameters.get('update_mode', 'update')
			#entity = civicrm.createOrUpdate(entity_type, record, update_mode)
			#civicrm.log(u"Wrote contact address for '%s'" % unicode(str(entity), 'utf8'),
			#	logging.INFO, 'importer', 'import_contact_address', 'Address', entity.get('id'), None, time.time()-timestamp)


def import_contact_base(civicrm, record_source, parameters=dict()):
	"""
	Imports very basic contact data, using the records' 'external_identifier' or 'id'
	as identification.

	parameters['update_mode'] can be set to anything CiviCRM.createOrUpdate accepts
	"""
	_prepare_parameters(parameters)
	timestamp = time.time()
	entity_type = parameters.get('entity_type', 'Contact')
	update_mode = parameters.get('update_mode', 'update')
	for record in record_source:
		entity = civicrm.createOrUpdate(entity_type, record, update_mode)
		civicrm.log(u"Wrote base contact '%s'" % unicode(str(entity), 'utf8'),
			logging.INFO, 'importer', 'import_contact_base', 'Contact', entity.get('id'), None, time.time()-timestamp)


def import_contact_phone(civicrm, record_source, parameters=dict()):
	"""
	Imports contact phone numbers.

	Expects the fields:
	"phone", "phone_type", "location_type"
	and identification ('id', 'external_identifier', 'contact_id')
	"""
	_prepare_parameters(parameters)
	for record in record_source:
		timestamp = time.time()
		record['contact_id'] = civicrm.getContactID(record)
		if not record['contact_id']:
			civicrm.log(u"Could not write contact phone, contact not found for '%s'" % str(record),
				logging.WARN, 'importer', 'import_contact_phone', 'Phone', None, None, time.time()-timestamp)
			continue

		try:
			number = civicrm.getPhoneNumber(record)
		except:
			number = None
			civicrm.logException("Exception while updating phone number for [%s]. Data was %s, exception: " % (record['contact_id'], str(record)),
				logging.ERROR, 'importer', 'import_contact_phone', 'Phone', None, record['contact_id'], time.time()-timestamp)

		if number:
			del record['location_type']
			del record['phone_type']
			del record['external_identifier']
			changed = number.update(record, store=True)
			if changed:
				civicrm.log("Updated phone number: %s" % str(number),
					logging.INFO, 'importer', 'import_contact_phone', 'Phone', number.get('id'), record['contact_id'], time.time()-timestamp)
			else:
				civicrm.log("Nothing changed for phone number: %s" % str(number),
					logging.INFO, 'importer', 'import_contact_phone', 'Phone', number.get('id'), record['contact_id'], time.time()-timestamp)

		else:
			number = civicrm.createPhoneNumber(record)
			civicrm.log("Created phone number: %s" % str(number),
				logging.INFO, 'importer', 'import_contact_phone', 'Phone', number.get('id'), record['contact_id'], time.time()-timestamp)


def import_contact_greeting(civicrm, record_source, parameters=dict()):
	"""
	Imports contact greeting settings

	Expects the fields:
	"postal_greeting", "postal_greeting_custom", "email_greeting", "email_greeting_custom"
	and identification ('id', 'external_identifier', 'contact_id')
	"""
	_prepare_parameters(parameters)
	for record in record_source:
		timestamp = time.time()
		contact = civicrm.getEntity(entity_type.CONTACT, record)
		if not contact:
			civicrm.log(u"Could not write contact greeting, contact not found for '%s'" % unicode(str(contact), 'utf8'),
				logging.WARN, 'importer', 'import_contact_greeting', 'Contact', None, None, time.time()-timestamp)
			continue
		
		update = dict(record)
		if update.has_key('postal_greeting'):
			if update['postal_greeting']:
				update['postal_greeting_id'] = civicrm.getOptionValueID(civicrm.getOptionGroupID('postal_greeting'), update['postal_greeting'])
			del update['postal_greeting']
		if update.has_key('email_greeting'):
			if update['email_greeting']:
				update['email_greeting_id'] = civicrm.getOptionValueID(civicrm.getOptionGroupID('email_greeting'), update['email_greeting'])
			del update['email_greeting']

		changed = contact.update(update, True)
		if changed:
			civicrm.log(u"Updated greeting settings for contact: %s" % unicode(str(contact), 'utf8'),
				logging.INFO, 'importer', 'import_contact_greeting', 'Contact', contact.get('id'), None, time.time()-timestamp)
		else:
			civicrm.log(u"Greeting settings not changed for contact: %s" % unicode(str(contact), 'utf8'),
				logging.INFO, 'importer', 'import_contact_greeting', 'Contact', contact.get('id'), None, time.time()-timestamp)


def import_contact_email(civicrm, record_source, parameters=dict()):
	"""
	Imports contact email address

	Expects the fields:
	"email", "location_type"
	and identification ('id', 'external_identifier', 'contact_id')
	"""
	_prepare_parameters(parameters)
	for record in record_source:
		timestamp = time.time()
		record['contact_id'] = civicrm.getContactID(record)
		if not record['contact_id']:
			civicrm.log(u"Could not write contact email, contact not found for '%s'" % str(record),
				logging.WARN, 'importer', 'import_contact_email', 'Email', None, None, time.time()-timestamp)
			continue

		# get the location type id
		if (not record.has_key('location_type_id')):
			location_type = record.get('location_type', parameters.get('location_type', 'Main'))
			location_type_dict = _get_or_create_from_params('location_type_dict', parameters)

			if location_type_dict.has_key(location_type):
				record['location_type_id'] = location_type_dict[location_type]
			else:
				record['location_type_id'] = civicrm.getLocationTypeID(location_type)
				location_type_dict[location_type] = record['location_type_id']
			if not record['location_type_id']:
				civicrm.log(u"Could not write contact email, location type %s could not be resolved" % location_type,
					logging.WARN, 'importer', 'import_contact_email', 'Email', None, None, time.time()-timestamp)
				continue


		number = civicrm.getEmail(record['contact_id'], record['location_type_id'])
		if number:
			del record['location_type']
			del record['location_type_id']
			del record['external_identifier']
			changed = number.update(record, store=True)
			if changed:
				civicrm.log("Updated email address: %s" % str(number),
					logging.INFO, 'importer', 'import_contact_email', 'Email', number.get('id'), record['contact_id'], time.time()-timestamp)
			else:
				civicrm.log("Nothing changed for phone number: %s" % str(number),
					logging.INFO, 'importer', 'import_contact_email', 'Email', number.get('id'), record['contact_id'], time.time()-timestamp)

		else:
			number = civicrm.createEmail(record['contact_id'], record['location_type_id'], record['email'])
			civicrm.log("Created email address: %s" % str(number),
				logging.INFO, 'importer', 'import_contact_email', 'Email', number.get('id'), record['contact_id'], time.time()-timestamp)


def import_membership(civicrm, record_source, parameters=dict()):
	"""
	Imports memberships

	Expects the fields:
              u'membership_type_id',
              u'status_id' | u'status'
              u'join_date',
              u'start_date',
              u'end_date',
	and identification ('id', 'external_identifier', 'contact_id')
	"""
	_prepare_parameters(parameters)
	for record in record_source:
		timestamp = time.time()
		record['contact_id'] = civicrm.getContactID(record)
		if not record['contact_id']:
			civicrm.log(u"Could not write membership, contact not found for '%s'" % str(record),
				logging.WARN, 'importer', 'import_membership', 'Membership', None, None, time.time()-timestamp)
			continue
		
		record['is_override'] = 1 	# write status as-is
		if record.has_key('status'):
			status_id = civicrm.getMembershipStatusID(record['status'])
			if not status_id:
				civicrm.log(u"Membership status '%s' does not exist!" % record['status'],
					logging.WARN, 'importer', 'import_membership', 'Membership', None, None, time.time()-timestamp)
				continue

			record['status_id'] = status_id
			del record['status']

		try:
			membership = civicrm.createOrUpdate('Membership', record, update_type='update', primary_attributes=[u'contact_id'])
			civicrm.log("Created membership: %s" % str(membership),
				logging.INFO, 'importer', 'import_membership', 'Membership', membership.get('id'), record['contact_id'], time.time()-timestamp)
		except:
			civicrm.log("Failed to create membership for contact: %s" % record['contact_id'],
				logging.ERROR, 'importer', 'import_membership', 'Membership', None, record['contact_id'], time.time()-timestamp)


def import_contact_groups(civicrm, record_source, parameters=dict()):
	_prepare_parameters(parameters)
	entity_type = parameters.get('entity_type', 'Contact')
	key_fields = parameters.get('key_fields', ['id', 'external_identifier'])

	for record in record_source:
		contact_id = civicrm.getContactID(record)
		if not contact_id:
			civicrm.log("Contact not found: ID %s" % contact_id,
				logging.WARN, 'importer', 'import_contact_groups', 'Contact', contact_id, None, 0)
			continue

		group_ids = parameters.get('group_ids', None)
		if group_ids==None:	# GET THE TAG IDS!
			parameters_lock = parameters['lock']
			parameters_lock.acquire()
			# test again, maybe another thread already created them...
			group_ids = parameters.get('group_ids', None)
			if group_ids==None:
				# no? ok, then it's up to us to query the tag ids
				group_ids = dict()
				for group_name in record.keys():
					if group_name in key_fields:
						continue
					group_id = civicrm.getOrCreateGroupID(group_name)
					group_ids[group_name] = group_id
					civicrm.log("Group '%s' has ID %s" % (group_name, group_id),
						logging.INFO, 'importer', 'import_contact_groups', 'GroupContact', group_id, None, 0)
				parameters['group_ids'] = group_ids
			parameters_lock.notifyAll()
			parameters_lock.release()

		currentGroups = civicrm.getContactGroupIds(contact_id)
		groups2change = dict()
		for group_name in record.keys():
			if not (group_name in key_fields):
				desiredState = (record[group_name].lower() in ['true', 1, '1', 'x', 'yes', 'y', 'ja', 'j'])
				currentState = (group_ids[group_name] in currentGroups)
				if currentState != desiredState:
					groups2change[group_ids[group_name]] = desiredState
		if groups2change:
			civicrm.log("Modifying groups for contact %s" % contact_id,
				logging.INFO, 'importer', 'import_contact_groups', 'Contact', contact_id, None, 0)
			for tag_id in groups2change:
				civicrm.setGroupMembership(contact_id, tag_id, groups2change[tag_id])
		else:
			civicrm.log("Groups are up to date for contact %s" % contact_id,
				logging.INFO, 'importer', 'import_contact_groups', 'Contact', contact_id, None, 0)
		


def import_contact_tags(civicrm, record_source, parameters=dict()):
	"""

	"""
	_prepare_parameters(parameters)
	entity_type = parameters.get('entity_type', 'Contact')
	key_fields = parameters.get('key_fields', ['id', 'external_identifier'])

	for record in record_source:
		contact_id = civicrm.getContactID(record)
		if not contact_id:
			civicrm.log("Contact not found: ID %s" % contact_id,
				logging.WARN, 'importer', 'import_contact_tags', 'Contact', contact_id, None, 0)
			continue

		tag_ids = parameters.get('tag_ids', None)
		if tag_ids==None:	# GET THE TAG IDS!
			parameters_lock = parameters['lock']
			parameters_lock.acquire()
			# test again, maybe another thread already created them...
			tag_ids = parameters.get('tag_ids', None)
			if tag_ids==None:
				# no? ok, then it's up to us to query the tag ids
				tag_ids = dict()
				for tag_name in record.keys():
					tag_id = civicrm.getOrCreateTagID(tag_name)
					tag_ids[tag_name] = tag_id
					civicrm.log("Tag '%s' has ID %s" % (tag_name, tag_id),
						logging.INFO, 'importer', 'import_contact_tags', 'EntityTag', tag_id, None, 0)
				parameters['tag_ids'] = tag_ids
			parameters_lock.notifyAll()
			parameters_lock.release()

		currentTags = civicrm.getContactTagIds(contact_id)
		tags2change = dict()
		for tag_name in record.keys():
			if not (tag_name in key_fields):
				desiredState = (record[tag_name].lower() in ['true', 1, '1', 'x', 'yes', 'y', 'ja', 'j'])
				currentState = (tag_ids[tag_name] in currentTags)
				if currentState != desiredState:
					tags2change[tag_ids[tag_name]] = desiredState
		if tags2change:
			civicrm.log("Modifying tags for contact %s" % contact_id,
				logging.INFO, 'importer', 'import_contact_tags', 'Contact', contact_id, None, 0)
			for tag_id in tags2change:
				civicrm.tagContact(contact_id, tag_id, tags2change[tag_id])
		else:
			civicrm.log("Tags are up to date for contact %s" % contact_id,
				logging.INFO, 'importer', 'import_contact_tags', 'Contact', contact_id, None, 0)





def parallelize(civicrm, import_function, workers, record_source, parameters=dict()):
	_prepare_parameters(parameters)
	# if only on worker, just call directly
	if workers==1:
		import_function(civicrm, record_source, parameters)
		return

	# multithreaded
	timestamp = time.time()
	record_list = list()
	record_list_lock = threading.Condition()
	thread_list = list()

	# first fill the queue
	record_source_iterator = record_source.__iter__()
	for i in range(5 * workers):
		try:
			record_list.append(record_source_iterator.next())
		except:
			break

	# then start the threads
	class Worker(threading.Thread):
		def __init__(self, function, civicrm, parameters, record_list, record_list_lock):
			threading.Thread.__init__(self)
			self.civicrm = civicrm
			self.parameters = parameters
			self.function = function
			self.record_list = record_list
			self.record_list_lock = record_list_lock
			self.start()

		def run(self):
			active = True
			while active:
				if record_list_lock:
					record_list_lock.acquire()
				
				if len(record_list)>0:
					record = record_list.pop(0)
				else:
					active = False
					record = None

				if record_list_lock:
					record_list_lock.notifyAll()
					record_list_lock.release()

				if record:
					# execute standard function
					try:
						self.function(self.civicrm, [record], self.parameters)			
					except:
						civicrm.logException(u"Exception caught for '%s' on procedure '%s'. Exception was: " % (threading.currentThread().name, import_function.__name__),
							logging.ERROR, 'importer', import_function.__name__, None, None, None, time.time()-timestamp)
						civicrm.log(u"Failed record was: %s" % str(record),
							logging.ERROR, 'importer', import_function.__name__, None, None, None, time.time()-timestamp)



	for i in range(workers):
		thread_list.append(Worker(import_function, civicrm, parameters, record_list, record_list_lock))

	# finally, feed the queue
	remaining_records = True
	while remaining_records:
		record_list_lock.acquire()
		record_list_lock.wait()
		try:
			record_list.append(record_source_iterator.next())
		except:
			remaining_records = False
		record_list_lock.release()
	
	for worker in thread_list:
		if worker.isAlive():
			worker.join()

	civicrm.log(u"Parallelized procedure '%s' completed." % import_function.__name__,
		logging.INFO, 'importer', 'parallelize', None, None, None, time.time()-timestamp)

