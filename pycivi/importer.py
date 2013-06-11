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




def import_base(civicrm, entity_type, record_source, workers=1):
	if workers==1:
		for record in record_source:
			_import_base(civicrm, entity_type, [record], None)
	else:
		# multithreaded
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
			def __init__(self, civicrm, entity_type, record_list, record_list_lock):
				threading.Thread.__init__(self)
				self.civicrm = civicrm
				self.entity_type = entity_type
				self.record_list = record_list
				self.record_list_lock = record_list_lock
				self.start()

			def run(self):
				_import_base(self.civicrm, self.entity_type, self.record_list, self.record_list_lock)

		for i in range(workers):
			thread_list.append(Worker(civicrm, entity_type, record_list, record_list_lock))

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
		
def _import_base(civicrm, entity_type, record_list, record_list_lock):
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
			record['contact_type'] = u'Organization'
			entity = civicrm.createOrUpdate(entity_type, record, 'update')
			print "Written:", entity


