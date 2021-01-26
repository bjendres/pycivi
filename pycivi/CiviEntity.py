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

import entity_type
import logging

class CiviEntity:
    def __init__(self, entity_type, entity_id, civicrm, attributes=dict()):
        self.entity_type = entity_type
        self.attributes = attributes
        self.civicrm = civicrm
        self.attributes['id'] = entity_id

    def __str__(self):
        return (u'%s entity [%d]' % (self.entity_type, self.getInt('id'))).encode('utf8')

    def get(self, attribute_key, default_value=None):
        return self.attributes.get(attribute_key, default_value)

    def getInt(self, attribute_key):
        return int(self.attributes.get(attribute_key, -1))

    def getID(self):
        return self.get('id')

    def set(self, attribute_key, new_value):
        self.attributes[attribute_key] = new_value

    def _storeChanges(self, changed_attributes):
        if changed_attributes:
            request = dict(changed_attributes)
            request['action'] = 'create'
            request['entity'] = self.entity_type
            request['id'] = self.attributes['id']
            self.civicrm.performAPICall(request)

    # update all provided attributes.
    def update(self, attributes, store=False):
        changed = dict()
        for key in attributes.keys():
            if (self.attributes.get(key, None)!=attributes[key]):
                self.attributes[key] = attributes[key]
                changed[key] = self.attributes[key]
        if store:
            self._storeChanges(changed)
        return changed

    # fill all provided attributes, i.e. do not overwrite any data, only set the ones that hadn't been set before
    def fill(self, attributes, store=False):
        changed = dict()
        for key in attributes.keys():
            if not self.attributes.get(key, None):
                self.attributes[key] = attributes[key]
                changed[key] = self.attributes[key]
        if store:
            self._storeChanges(changed)
        return changed


    # update all provided attributes, but don't add the ones that were not there
    def replace(self, attributes, store=False):
        changed = dict()
        for key in attributes.keys():
            if self.attributes.has_key(key):
                if (self.attributes[key]!=attributes[key]):
                    self.attributes[key] = attributes[key]
                    changed[key] = self.attributes[key]
        if store:
            self._storeChanges(changed)
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
            self._storeChanges(changes)
            civi.log("Stored changes to '%s'" % unicode(str(self), 'utf8'), logging.INFO)
        else:
            civi.log("No changes have been made, not storing '%s'" % unicode(str(self), 'utf8'), logging.INFO)


    def delete(self, final=True, civi=None):
        if civi==None: civi = self.civicrm
        civi.performAPICall({'entity':self.entity_type, 'action':'delete', 'id':self.attributes['id']})



class CiviTaggableEntity(CiviEntity):
    pass


class CiviContactEntity(CiviTaggableEntity):
    def __str__(self):
        return (u'%s [%s]' % (self.get('display_name'), self.get('id'))).encode('utf8')

    def isType(self, type):
        """
        test if the contact has a certain type
        """
        return self.get('contact_type')==type

    def delete(self, final=True, civi=None):
        """
        deleting a contact can be more tricky than other entities...
        """
        if civi==None: civi = self.civicrm
        query = {'entity':self.entity_type, 'action':'delete', 'id':self.attributes['id']}
        if final:
            query['skip_undelete'] = 1

            # make sure all pending contribtions get deleted....
            keep_going = True
            while keep_going:
                keep_going = False
                query_pending = {'entity':entity_type.CONTRIBUTION, 'action':'get', 'contact_id': self.get('id')}
                pending_contributions = civi.performAPICall(query_pending)
                for pending_contribution in pending_contributions['values']:
                    keep_going = True
                    entity = civi._createEntity(entity_type.CONTRIBUTION, pending_contribution)
                    print "Deleting related contribution", str(entity)
                    entity.delete()

        # now delte the contact
        civi.performAPICall(query)


    def convertToType(self, new_type):
        """
        convert to another contact type. Returns True if successfull
        """
        current_type = self.get('contact_type')
        if current_type == new_type:
            return True

        if current_type == 'Individual':
            if new_type == 'Organization':
                # Conversion: Individual -> Organization
                new_name = (self.get('first_name', '') + u' ' + self.get('last_name', '')).strip()
                self.set('contact_type', 'Organization')
                self.set('organization_name', new_name)
                self.set('first_name', '')
                self.set('last_name', '')
                self.set('middle_name', '')
                self.set('gender_id', '')
                self.set('current_employer', '')
                self.set('is_deceased', '')
                self.set('birth_date', '')
                self.set('job_title', '')
                return True

        elif current_type == 'Organization':
            if new_type == 'Individual':
                # Conversion: Organization -> Individual
                self.set('contact_type', 'Individual')
                self.set('contact_sub_type', '')

                # fix names
                organization_name = self.get('organization_name')
                if not self.get('last_name') and not self.get('last_name'):
                    # check, if organization name possibly holds first and last name
                    parts = organization_name.split(' ', 1)
                    if len(parts) > 1:
                        self.set('first_name', parts[0])
                        self.set('last_name', parts[1])
                    else:
                        self.set('first_name', '')
                        self.set('last_name', parts[0])

                elif not self.get('last_name'):
                    first_name = self.get('first_name')
                    if organization_name!=first_name and organization_name.startswith(first_name + ' '):
                        self.set('last_name', organization_name[len(first_name)+1:].strip())
                    else:
                        self.set('last_name', organization_name)

                elif not self.get('first_name'):
                    # simply no first name...can't be helped...
                    pass
                return True

        elif current_type == 'Household':
            if new_type == 'Individual':
                # Conversion: Household -> Individual
                self.set('contact_type', 'Individual')
                self.set('first_name', '')
                self.set('last_name', self.get('household_name'))
                self.set('household_name', '')
                self.set('primary_contact_id', '')
                self.set('middle_name', '')
                self.set('gender_id', '')
                self.set('current_employer', '')
                self.set('is_deceased', '')
                self.set('birth_date', '')
                self.set('job_title', '')
                return True


        self.civicrm.log("Unknown conversion '%s' => '%s'!" % (current_type, new_type), logging.ERROR)
        return False

    def updateDisplayName(self):
        """
        update display name
        """
        request = dict()
        request['action'] = 'update'
        request['entity'] = self.entity_type
        request['id'] = self.get('id')
        self.civicrm.performAPICall(request, {'forcePost': True})


class CiviPhoneEntity(CiviEntity):
    def __str__(self):
        return (u"%s:'%s'" % (self.get('phone_type', "#"), self.get('phone'))).encode('utf8')


class CiviCampaignEntity(CiviEntity):
    def __str__(self):
        return (u"Campaign (%s): \"%s\"" % (self.get('id'), self.get('title'))).encode('utf8')


class CiviContributionEntity(CiviTaggableEntity):
    def __str__(self):
        return (u'Contribution [%s]' % self.get('id')).encode('utf8')

    def _storeChanges(self, changed_attributes):
        if changed_attributes:
            # we have to submit the contact ID in any case, so that an activity can be produced!
            if not 'contact_id' in changed_attributes:
                changed_attributes['contact_id'] = self.get('contact_id')

            # we also have to submit the currency in any case
            if not 'currency' in changed_attributes:
                changed_attributes['currency'] = self.get('currency')

            # we have to submit the status ID, otherwise it will default regardless of the current status
            if not 'contribution_status_id' in changed_attributes:
                changed_attributes['contribution_status_id'] = self.get('contribution_status_id')
            return CiviTaggableEntity._storeChanges(self, changed_attributes)
        return dict()


class CiviNoteEntity(CiviTaggableEntity):
    def _storeChanges(self, changed_attributes):
        if changed_attributes:
            # we have to submit the entity_id
            if not 'entity_id' in changed_attributes:
                changed_attributes['entity_id'] = self.get('entity_id')

            # ...and entity_table
            if not 'entity_table' in changed_attributes:
                changed_attributes['entity_table'] = self.get('entity_table')

            return CiviTaggableEntity._storeChanges(self, changed_attributes)
        return dict()


class CiviRelationshipTypeEntity(CiviEntity):
    def createOrUpdateRelation(self, contact_a_id, contact_b_id, parameters=dict()):
        '''
        will create a relationship if it does not already exist
        '''
        parameters['contact_id_a'] = contact_a_id
        parameters['contact_id_b'] = contact_b_id
        parameters['relationship_type_id'] = self.getID()
        relation = self.civicrm.createOrUpdate('Relationship', parameters, 'update', ['relationship_type_id', 'contact_id_a', 'contact_id_b'])


class CiviAddressEntity(CiviEntity):
    def shareWith(self, contact_id):
        '''
        share the address with the given contact
        '''
        new_address_data = dict(self.attributes)
        new_address_data['master_id'] = self.getID()
        new_address_data['contact_id'] = contact_id
        del new_address_data['id']
        return self.civicrm.createEntity(self.entity_type, new_address_data)


class CiviEmailEntity(CiviEntity):
    # update all provided attributes.
    # FIX for Civicrm-4.3.7:
    # We need to provide all attributes of the entity for an update
    def _storeChanges(self, changed_attributes):
        if changed_attributes:
            # we have to submit the entity_id
            if not 'email' in changed_attributes:
                changed_attributes['email'] = self.get('email')
            return CiviEntity._storeChanges(self, changed_attributes)
        return dict()
