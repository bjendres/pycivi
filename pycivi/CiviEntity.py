
class CiviEntity:
	def __init__(self, entity_type, entity_id, civicrm, attributes=dict()):
		self.entity_type = entity_type
		self.attributes = attributes
		self.civicrm = civicrm
		self.attributes['id'] = entity_id

	def __str__(self):
		return (u'%s entity [%d]' % (self.entity_type, self.getInt('id'))).encode('utf8')

	def get(self, attribute_key):
		return self.attributes.get(attribute_key, None)

	def getInt(self, attribute_key):
		return int(self.attributes.get(attribute_key, -1))

	# update all provided attributes.
	def update(self, attributes):
		changed = False
		for key in attributes.keys():
			changed |= (self.attributes.get(key, None)!=attributes[key])
			self.attributes[key] = attributes[key]
		return changed


	# fill all provided attributes, i.e. do not overwrite any data, only set the ones that hadn't been set before
	def fill(self, attributes):
		changed = False
		for key in attributes.keys():
			if not self.attributes.has_key(key):
				changed = True
				self.attributes[key] = attributes[key]
		return changed


	# update all provided attributes, but don't add the ones that were not there
	def replace(self, attributes):
		changed = False
		for key in attributes.keys():
			if self.attributes.has_key(key):
				changed |= (self.attributes[key]!=attributes[key])
				self.attributes[key] = attributes[key]
		return changed


	def reload(self, civi=None):
		if civi==None: civi = self.civicrm
		result = civi.performAPICall({'entity':self.entity_type, 'action':'get', 'id':self.attributes['id']})
		self.attributes = result['values'][0]

	
	def store(self, civi=None):
		if civi==None: civi = self.civicrm
		result = civi.performAPICall({'entity':self.entity_type, 'action':'get', 'id':self.attributes['id']})
		current_state = result['values'][0] 

		# find the fields that have changed
		changes = dict()
		for key in self.attributes:
			if not current_state.has_key(key) or self.attributes[key]!=current_state[key]:
				changes[key] = self.attributes[key]
		
		if changes:
			changes['entity'] = self.entity_type
			changes['action'] = 'create'
			changes['id'] = self.attributes['id']
			civi.performAPICall(changes)
			civi.logger.info("Stored changes to '%s'" % str(self))
		else:
			civi.logger.info("No changes have been made, not storing '%s'" % str(self))



class CiviTaggableEntity(CiviEntity):
	pass


class CiviContactEntity(CiviTaggableEntity):
	def __str__(self):
		return (u'%s [%d]' % (self.get('display_name'), self.getInt('id'))).encode('utf8')

