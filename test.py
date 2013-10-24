import time
import random
import string

from pycivi import CiviCRM_DRUSH, CiviCRM_REST

def test(civi, names):
	timestamp = time.time()
	for name in names:
		civi.performAPICall({'entity':'Contact', 'action':'getquick', 'name':name})
	return time.time()-timestamp

# create names
strings = []
for i in range(100):
	strings.append(''.join(random.choice(string.ascii_uppercase) for x in range(3)))

print "Running getquick for %d strings..." % len(strings)

drush = CiviCRM_DRUSH.CiviCRM_DRUSH('~/Documents/mamp_root/mh', '~/Documents/workspace/drush/drush')
runtime = test(drush, strings)
print "Drush took %fs" % runtime

rest = CiviCRM_REST.CiviCRM_REST("http://localhost:8888/mh", "b89b7f646362f508afec2c6c814ce356", "xAt5IjHRK5")
runtime = test(rest, strings)
print "REST took %fs" % runtime
