import CiviCRM, entity_type
import csv
import codecs

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
				record[field] = row[i]
			return record




def import_base(civicrm, entity_type, record_source):
	for record in record_source:
		record['contact_type'] = u'Organization'
		entity = civicrm.getOrCreate(entity_type, record)
		print "Written:", entity
		#print record


