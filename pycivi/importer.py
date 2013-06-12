import CiviCRM, entity_type
import csv
import codecs
import threading

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
	def __init__(self, csv_file, mapping=dict(), delimiter=','):
		inputStream = open(csv_file, 'rb')
		self.reader = UnicodeReader(inputStream, 'excel', 'utf8', delimiter=delimiter, quotechar='"')
		self.mapping = mapping
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
				if field in self.mapping:
					field = self.mapping[field]
				if field!=None:
					record[field] = row[i]
			return record





def import_contact_base(civicrm, record_source, parameters):
	"""
	Imports very basic contact data, using the records' 'external_identifier' or 'id'
	as identification.

	parameters['update_mode'] can be set to anything CiviCRM.createOrUpdate accepts
	"""
	entity_type = parameters.get('entity_type', 'Contact')
	update_mode = parameters.get('update_mode', 'update')
	for record in record_source:
		entity = civicrm.createOrUpdate(entity_type, record, update_mode)
		print "Written:", entity


def import_contact_tags(civicrm, record_source, parameters):
	"""

	"""
	entity_type = parameters.get('entity_type', 'Contact')
	key_fields = parameters.get('key_fields', ['id', 'external_identifier'])

	for record in record_source:
		contact_id = civicrm.getContactID(record)
		if not contact_id:
			print "Contact not found!"
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
					print "Mapped tag '%s' to #%s" % (tag_name, tag_id)
				parameters['tag_ids'] = tag_ids
			parameters_lock.notifyAll()
			parameters_lock.release()

		for tag_name in record.keys():
			if not (tag_name in key_fields):
				setTag = (record[tag_name].lower() in ['true', 1, '1', 'x', 'yes', 'y', 'ja', 'j'])
				civicrm.tagContact(contact_id, tag_ids[tag_name], setTag)




def parallelize(civicrm, import_function, workers, record_source, parameters=dict()):
	# multithreaded
	record_list = list()
	record_list_lock = threading.Condition()
	thread_list = list()
	parameters['lock'] = threading.Condition()

	# first fill the queue
	record_source_iterator = record_source.__iter__()
	for i in range(5 * workers):
		try:
			record_list.append(record_source_iterator.next())
		except:
			break

	# then start the threads
	class Worker(threading.Thread):
		#def __init__(self, civicrm, entity_type, record_list, record_list_lock):
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

				if record_list_lock:
					record_list_lock.notifyAll()
					record_list_lock.release()

				if record:
					# execute standard function
					self.function(self.civicrm, [record], self.parameters)			

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
		worker.join()
	print "Done"


