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


from . import CiviCRM, entity_type
import csv
import codecs
import threading
import logging
import time
import traceback
import datetime

from .CiviCRM import CiviAPIException


class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def __next__(self):
        return self.reader.next().encode("utf-8")

class UnicodeReader:
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        f = UTF8Recoder(f, encoding)
        self.reader = csv.reader(f, dialect=dialect, **kwds)

    def __next__(self):
        row = next(self.reader)
        return [str(s, "utf-8") for s in row]

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
        self.header = next(self.row_iterator)
        return self

    def __next__(self):
        if self.row_iterator:
            row = next(self.row_iterator)

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
    if 'lock' not in parameters:
        parameters['lock'] = threading.Condition()


def import_contributions(civicrm, record_source, parameters=dict()):
    """
    Imports import_contributions

    parameters['update_mode'] can be set to anything CiviCRM.createOrUpdate accepts
    parameters['id'] can be set to the identifying field (e.g. 'external_identifier' or 'trxn_id')
    parameters['campaign_identifier'] can be set to set to identify the campaign. Default is 'title'
    parameters['fallback_contact'] can be set to  provide a default fallback contact ID (e.g. "Unkown Donor")
    """
    _prepare_parameters(parameters)
    timestamp = time.time()
    entity_type = parameters.get('entity_type', 'Contribution')
    update_mode = parameters.get('update_mode', 'update')
    campaign_identifier = parameters.get('campaign_identifier', 'title')
    for record in record_source:
        update = dict(record)
        # lookup contact_id
        if 'contact_external_identifier' in update:
            if update['contact_external_identifier']:
                update['contact_id'] = civicrm.getContactID({'external_identifier': update['contact_external_identifier']})
            del update['contact_external_identifier']
        if 'contact_id' not in update or not update['contact_id']:

            if 'fallback_contact' in parameters:
                update['contact_id'] = parameters['fallback_contact']
                civicrm.log("Contact not found! Will be attributed to fallback contact %s" % str(parameters['fallback_contact']),
                    logging.INFO, 'importer', 'import_contributions', 'Contribution', None, None, time.time()-timestamp)
            else:
                civicrm.log("Contact not found! No valid contact reference specified in (%s)" % str(str(record), 'utf8'),
                    logging.ERROR, 'importer', 'import_contributions', 'Contribution', None, None, time.time()-timestamp)
                continue

        # lookup payment type
        if 'payment_instrument' in update:
            if update['payment_instrument']:
                update['payment_instrument_id'] = civicrm.getOptionValue(civicrm.getOptionGroupID('payment_instrument'), update['payment_instrument'])
            del update['payment_instrument']
        if 'payment_instrument_id' not in update or not update['payment_instrument_id']:
            civicrm.log("Payment type ID not found! No valid payment type specified in (%s)" % str(str(record), 'utf8'),
                logging.ERROR, 'importer', 'import_contributions', 'Contribution', None, None, time.time()-timestamp)
            continue

        # lookup campaign
        if 'contribution_campaign' in update:
            if update['contribution_campaign']:
                update['contribution_campaign_id'] = civicrm.getCampaignID(update['contribution_campaign'], attribute_key=campaign_identifier)
                if not update['contribution_campaign_id']:
                    civicrm.log("Campaign ID not found! No valid campaign specified in (%s)" % str(str(record), 'utf8'),
                        logging.WARN, 'importer', 'import_contributions', 'Contribution', None, None, time.time()-timestamp)
            del update['contribution_campaign']

        # lookup contribution status
        if 'contribution_status' in update:
            if update['contribution_status']:
                update['contribution_status_id'] = civicrm.getOptionValue(civicrm.getOptionGroupID('contribution_status'), update['contribution_status'])
            del update['contribution_status']
        if 'contribution_status_id' not in update or not update['contribution_status_id']:
            civicrm.log("Contribution status ID not found! No valid contribution status specified in (%s)" % str(str(record), 'utf8'),
                logging.ERROR, 'importer', 'import_contributions', 'Contribution', None, None, time.time()-timestamp)
            continue

        entity = civicrm.createOrUpdate(entity_type, update, update_mode, ['id', 'trxn_id'])
        civicrm.log("Wrote contribution '%s'" % str(str(entity), 'utf8'),
            logging.INFO, 'importer', 'import_contributions', 'Contribution', entity.get('id'), None, time.time()-timestamp)


def import_rcontributions(civicrm, record_source, parameters=dict()):
    """
    Imports recurring contributions

    parameters['update_mode'] can be set to anything CiviCRM.createOrUpdate accepts
    parameters['identification'] can be set to the identifying fields []
    parameters['campaign_identifier'] can be set to set to identify the campaign. Default is 'title'
    parameters['fallback_contact'] can be set to  provide a default fallback contact ID (e.g. "Unkown Donor")
    """
    _prepare_parameters(parameters)
    timestamp = time.time()
    entity_type = parameters.get('entity_type', 'ContributionRecur')
    update_mode = parameters.get('update_mode', 'update')
    campaign_identifier = parameters.get('campaign_identifier', 'title')
    identification = parameters.get('identification', ['id'])

    for record in record_source:
        update = dict(record)
        # lookup contact_id
        if 'contact_external_identifier' in update:
            if update['contact_external_identifier']:
                update['contact_id'] = civicrm.getContactID({'external_identifier': update['contact_external_identifier']})
            del update['contact_external_identifier']
        if 'contact_id' not in update or not update['contact_id']:

            if 'fallback_contact' in parameters:
                update['contact_id'] = parameters['fallback_contact']
                civicrm.log("Contact not found! Will be attributed to fallback contact %s" % str(parameters['fallback_contact']),
                    logging.INFO, 'importer', 'import_contributions', 'Contribution', None, None, time.time()-timestamp)
            else:
                civicrm.log("Contact not found! No valid contact reference specified in (%s)" % str(str(record), 'utf8'),
                    logging.ERROR, 'importer', 'import_contributions', 'Contribution', None, None, time.time()-timestamp)
                continue

        # lookup payment type
        if 'payment_instrument' in update:
            if update['payment_instrument']:
                update['payment_instrument_id'] = civicrm.getOptionValue(civicrm.getOptionGroupID('payment_instrument'), update['payment_instrument'])
            del update['payment_instrument']
        if 'payment_instrument_id' not in update or not update['payment_instrument_id']:
            civicrm.log("Payment type ID not found! No valid payment type specified in (%s)" % str(str(record), 'utf8'),
                logging.ERROR, 'importer', 'import_contributions', 'Contribution', None, None, time.time()-timestamp)
            continue

        # lookup campaign
        if 'contribution_campaign' in update:
            if update['contribution_campaign']:
                update['contribution_campaign_id'] = civicrm.getCampaignID(update['contribution_campaign'], attribute_key=campaign_identifier)
                if not update['contribution_campaign_id']:
                    civicrm.log("Campaign ID not found! No valid campaign specified in (%s)" % str(str(record), 'utf8'),
                        logging.WARN, 'importer', 'import_contributions', 'Contribution', None, None, time.time()-timestamp)
            del update['contribution_campaign']

        # lookup contribution status
        if 'contribution_status' in update:
            if update['contribution_status']:
                update['contribution_status_id'] = civicrm.getOptionValue(civicrm.getOptionGroupID('contribution_status'), update['contribution_status'])
            del update['contribution_status']
        if 'contribution_status_id' not in update or not update['contribution_status_id']:
            civicrm.log("Contribution status ID not found! No valid contribution status specified in (%s)" % str(str(record), 'utf8'),
                logging.ERROR, 'importer', 'import_contributions', 'Contribution', None, None, time.time()-timestamp)
            continue

        entity = civicrm.createOrUpdate(entity_type, update, update_mode, identification)
        civicrm.log("Wrote recurring contribution '%s'" % str(str(entity), 'utf8'),
            logging.INFO, 'importer', 'import_rcontributions', 'ContributionRecur', entity.get('id'), None, time.time()-timestamp)


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
        if 'campaign_type' in update:
            if update['campaign_type']:
                update['campaign_type_id'] = civicrm.getOptionValue(civicrm.getOptionGroupID('campaign_type'), update['campaign_type'])
            del update['campaign_type']
        if 'campaign_type_id' not in update or not update['campaign_type_id']:
            civicrm.log("Campaign type ID not identified! No valid campaign type specified in (%s)" % str(str(record), 'utf8'),
                logging.ERROR, 'importer', 'import_campaigns', 'Campaign', None, None, time.time()-timestamp)
            continue

        # lookup campaign status
        if 'status' in update:
            if update['status']:
                update['status_id'] = civicrm.getOptionValue(civicrm.getOptionGroupID('campaign_status'), update['status'])
            del update['status']
        if 'campaign_type_id' not in update or not update['campaign_type_id']:
            civicrm.log("Campaign status ID not identified! No valid campaign status specified in (%s)" % str(str(record), 'utf8'),
                logging.ERROR, 'importer', 'import_campaigns', 'Campaign', None, None, time.time()-timestamp)
            continue

        if 'id' in parameters:
            entity = civicrm.createOrUpdate(entity_type, update, update_mode, [parameters['id']])
        else:
            entity = civicrm.createOrUpdate(entity_type, update, update_mode)
        civicrm.log("Wrote campaign '%s'" % str(str(entity), 'utf8'),
            logging.INFO, 'importer', 'import_campaign', 'Campaign', entity.get('id'), None, time.time()-timestamp)



def import_notes(civicrm, record_source, parameters=dict()):
    """
    Imports notes

    Expects the fields:
              'entity_table'
              'entity_id'
              'note'
              'subject'
              'privacy'

    It will also resolve the entity_type:
              'lookup_type'
              'lookup_identifier_key'
              'lookup_identifier_value'
        to generate entity_table and entity_id

    the parameters can contain
        'mode' = 'add' (default)    - will always add a new note
        'mode' = 'replace_subject'  - will replace a note with the same subject
    """
    _prepare_parameters(parameters)
    for record in record_source:
        timestamp = time.time()
        if 'lookup_type' in record and 'lookup_identifier_key' in record and 'lookup_identifier_value' in record:
            # will lookup the related entity
            entity_type = record['lookup_type']
            entity_lookup_key = record['lookup_identifier_key']
            entity_lookup_value = record['lookup_identifier_value']

            civicrm.log("Looking up a %s with %s='%s'" % (entity_type, entity_lookup_key, entity_lookup_value),
                logging.INFO, 'importer', 'import_notes', 'Note', None, None, time.time()-timestamp)
            try:
                entity = civicrm.getEntity(entity_type, {entity_lookup_key: entity_lookup_value}, primary_attributes=[entity_lookup_key])
                if entity:
                    record['entity_table'] = 'civicrm_' + entity_type.lower()
                    record['entity_id'] = entity.get('id')
                else:
                    civicrm.log("Couldn't find a %s with %s='%s'" % (entity_type, entity_lookup_key, entity_lookup_value),
                        logging.ERROR, 'importer', 'import_notes', 'Note', None, Note, time.time()-timestamp)
            except:
                pass

        if not 'entity_id' in record or not 'entity_table' in record:
            civicrm.log("Failed to create note, missing target information entity_id and entity_table",
                logging.ERROR, 'importer', 'import_notes', 'Note', None, None, time.time()-timestamp)
            continue

        try:
            mode = parameters.get('mode', 'add')
            primary_attributes = ['entity_id', 'entity_table', 'id']
            if mode=='replace_subject':
                primary_attributes.append('subject')
            record.pop('lookup_identifier_key', None)
            record.pop('lookup_identifier_value', None)
            record.pop('lookup_type', None)
            try:
                note = civicrm.createOrUpdate('Note', record, update_type='update', primary_attributes=primary_attributes)
            except CiviAPIException as ex:
                civicrm.log("Failed to create/update note. Please make sure that GET/POST parameter length (e.g. PHP's suhosin.get.max_value_length) is greater than %d" % len(record['note']),
                    logging.ERROR, 'importer', 'import_notes', 'Note', None, None, time.time()-timestamp)
                raise ex

            civicrm.log("Created note: %s" % str(note),
                logging.INFO, 'importer', 'import_notes', 'Note', note.get('id'), record['entity_id'], time.time()-timestamp)
        except:
            civicrm.logException()
            civicrm.log("Failed to create note for entity: %s" % record['entity_id'],
                logging.ERROR, 'importer', 'import_notes', 'Note', None, record['entity_id'], time.time()-timestamp)


def import_contact_address(civicrm, record_source, parameters=dict()):
    """
    Makes sure, that the contact has the given address

    Possible record entries:
    "id", "contact_id", "location_type_id", "is_primary", "is_billing", "street_address",
    "supplemental_address_1", "supplemental_address_2", "city", "postal_code", "country_id",
    "manual_geo_code",

    Parameters:
    parameters['location_type']         sets the type (default "Main") if no information provided by record
    parameters['update_mode']             either set to "update", "fill" or "replace" - or "add" to create a new entry
    parameters['no_update']             if True we do not touch existing addresses at all
    """
    _prepare_parameters(parameters)
    no_update = parameters.get('no_update', False)
    for record in record_source:
        timestamp = time.time()
        record['contact_id'] = civicrm.getContactID(record)
        if not record['contact_id']:
            civicrm.log("Could not write contact address, contact not found for '%s'" % str(record),
                logging.WARN, 'importer', 'import_contact_address', 'Address', None, None, time.time()-timestamp)
            continue

        # get the location type id
        if ('location_type_id' not in record):
            location_type = record.get('location_type', parameters.get('location_type', 'Main'))
            location_type_dict = _get_or_create_from_params('location_type_dict', parameters)

            if location_type in location_type_dict:
                record['location_type_id'] = location_type_dict[location_type]
            else:
                record['location_type_id'] = civicrm.getLocationTypeID(location_type)
                location_type_dict[location_type] = record['location_type_id']

        if no_update:
            try:
                address = civicrm.createIfNotExists('Address', record, primary_attributes=['contact_id', 'location_type_id'])
            except:
                civicrm.logException("Exception while importing address for [%s]. Data was %s, exception: " % (record['contact_id'], str(record)),
                    logging.ERROR, 'importer', 'import_contact_address', 'Address', None, record['contact_id'], time.time()-timestamp)
            else:
                if address:
                    civicrm.log("Wrote contact address for '%s'" % str(str(address), 'utf8'),
                        logging.INFO, 'importer', 'import_contact_address', 'Address', address.get('id'), None, time.time()-timestamp)
                else:
                    civicrm.log("Contact-address already exists and was not updated for contact [%s]" % str(record['contact_id']),
                        logging.INFO, 'importer', 'import_contact_address', 'Address', None, None, time.time()-timestamp)
        else:
            mode = parameters.get('update_mode', 'update')
            if mode in ['update', 'fill', 'replace']:
                try:
                    address = civicrm.createOrUpdate('Address', record, update_type=mode, primary_attributes=['contact_id', 'location_type_id'])
                    civicrm.log("Wrote contact address for '%s'" % str(str(address), 'utf8'),
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
                #    logging.INFO, 'importer', 'import_contact_address', 'Address', entity.get('id'), None, time.time()-timestamp)


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
        civicrm.log("Wrote base contact '%s'" % str(str(entity), 'utf8'),
            logging.INFO, 'importer', 'import_contact_base', 'Contact', entity.get('id'), None, time.time()-timestamp)


def import_contact_with_dupe_check(civicrm, record_source, parameters=dict()):
    """
    Imports very basic contact data, using the records' 'external_identifier' or 'id'
    as identification.

    parameters['update_mode'] can be set to anything CiviCRM.createOrUpdate accepts
    parameters['update_mode'] defaults to 'fill'
    """
    _prepare_parameters(parameters)
    update_mode = parameters.get('update_mode', 'fill')
    timestamp = time.time()
    entity_type = parameters.get('entity_type', 'Contact')
    for record in record_source:
        query = dict()
        query['action'] = 'create'
        query['entity'] = entity_type
        query['dupe_check'] = 1
        query.update(record)
        result = civicrm.performSimpleAPICall(query)

        # we have a duplicate; lets update it
        if result['is_error'] == 1 and result.get('error_code') == 'duplicate':
            if len(result['ids']) == 1:
                record['id'] = result['ids'][0]
                entity = civicrm.createOrUpdate(entity_type, record, update_mode, ['id'])
                civicrm.log("Duplicate found and updated: '%s'" % str(str(entity), 'utf8'),
                    logging.INFO, 'importer', 'import_contact_with_dupe_check', 'Contact', entity.get('id'), None, time.time()-timestamp)
            else:
                civicrm.log("More than one duplicates found: {}".format(result['ids']),
                    logging.INFO, 'importer', 'import_contact_with_dupe_check', 'Contact', None, None, time.time()-timestamp)

        # there is already a contact with the given itendifiers (id or external_identifier)
        # we also update this contact...
        elif result['is_error'] == 1 and result['error_message'] == 'DB Error: already exists':
            entity = civicrm.createOrUpdate(entity_type, record, update_mode)
            civicrm.log("Contact identified and updated: '%s'" % str(str(entity), 'utf8'),
                logging.INFO, 'importer', 'import_contact_with_dupe_check', 'Contact', entity.get('id'), None, time.time()-timestamp)

        # an unkown error occured
        elif result['is_error'] == 1:
            civicrm.log("Error occured while trying to create a Contact. record: '{0}' | error_message: '{1}'".format(record, result.get('error_message', str())),
                logging.INFO, 'importer', 'import_contact_with_dupe_check', 'Contact', record.get('external_identifier'), None, time.time()-timestamp)

        # no matched or existing contact found; a new one was created
        else:
            civicrm.log("Wrote base contact '{first_name} {last_name} [{id}]'".format(**result['values'][0]),
                logging.INFO, 'importer', 'import_contact_base', 'Contact', result['id'], None, time.time()-timestamp)


def import_contact_website(civicrm, record_source, parameters=dict()):
    """
    Imports contact web sites.

    Expects the fields:
    "url", "website_type"
    and identification ('id', 'external_identifier', 'contact_id')

    parameters:
     multiple:    'allow' - allows multiple web sites

    """
    _prepare_parameters(parameters)
    multiple = parameters.get('multiple', False)
    for record in record_source:
        timestamp = time.time()
        record['contact_id'] = civicrm.getContactID(record)
        if not record['contact_id']:
            civicrm.log("Could not write contact website, contact not found for '%s'" % str(record),
                logging.WARN, 'importer', 'import_contact_website', 'Website', None, None, time.time()-timestamp)
            continue

        # get the website type id
        if ('website_type_id' not in record):
            if 'website_type' in record:
                if record['website_type']:
                    record['website_type_id'] = civicrm.getOptionValue(civicrm.getOptionGroupID('website_type'), record['website_type'])
                del record['website_type']

        if ('website_type_id' not in record):
            civicrm.log("Could not write contact website, website type '%s' could not be resolved" % record.get('website_type', ''),
                logging.WARN, 'importer', 'import_contact_website', 'Website', None, None, time.time()-timestamp)
            continue


        sites = civicrm.getWebsites(record['contact_id'], record['website_type_id'])

        if multiple=='allow':
            # allow multiple web sites of the same type

            # check if it is already there...
            already_there = False
            for site in sites:
                if record['url'].lower() == site.get('url').lower():
                    # it is already there
                    civicrm.log("No new websites for contact [%s]" % str(site.get('contact_id')),
                        logging.INFO, 'importer', 'import_contact_website', 'Website', site.get('id'), site.get('contact_id'), time.time()-timestamp)
                    already_there = True
                    break

            # nothing? then create new website record
            if not already_there:
                site = civicrm.createWebsite(record)
                civicrm.log("Added new website for contact [%s]" % str(site.get('contact_id')),
                    logging.INFO, 'importer', 'import_contact_website', 'Website', site.get('id'), site.get('contact_id'), time.time()-timestamp)

        else:
            if len(sites) > 0:
                site = sites[0]
            else:
                site = None

            if site:
                del record['website_type']
                del record['url']
                del record['external_identifier']
                changed = site.update(record, store=True)
                if changed:
                    if len(sites) > 1:
                        civicrm.log("More than one website set, modified first...",
                            logging.WARN, 'importer', 'import_contact_website', 'Website', site.get('id'), record['contact_id'], time.time()-timestamp)
                    civicrm.log("Updated website: %s" % str(site),
                        logging.INFO, 'importer', 'import_contact_website', 'Website', site.get('id'), record['contact_id'], time.time()-timestamp)
                else:
                    civicrm.log("Nothing changed for website: %s" % str(site),
                        logging.INFO, 'importer', 'import_contact_website', 'Website', site.get('id'), record['contact_id'], time.time()-timestamp)

            else:
                site = civicrm.createWebsite(record)
                civicrm.log("Added new website for contact [%s]" % str(site.get('contact_id')),
                    logging.INFO, 'importer', 'import_contact_website', 'Website', site.get('id'), site.get('contact_id'), time.time()-timestamp)



def import_contact_phone(civicrm, record_source, parameters=dict()):
    """
    Imports contact phone numbers.

    Expects the fields:
    "phone", "phone_type", "location_type"
    and identification ('id', 'external_identifier', 'contact_id')

    parameters:
     multiple:    'allow' - allows multiple phone numbers per type
     no_update: if True, existing phone-numbers won't be overwritten; default is False

    """
    _prepare_parameters(parameters)
    no_update = parameters.get('no_update', False)
    multiple = parameters.get('multiple', False)
    for record in record_source:
        timestamp = time.time()
        record['contact_id'] = civicrm.getContactID(record)
        if not record['contact_id']:
            civicrm.log("Could not write contact phone, contact not found for '%s'" % str(record),
                logging.WARN, 'importer', 'import_contact_phone', 'Phone', None, None, time.time()-timestamp)
            continue

        # get the location type id
        if ('location_type_id' not in record):
            location_type = record.get('location_type', parameters.get('location_type', 'Main'))
            location_type_dict = _get_or_create_from_params('location_type_dict', parameters)

            if location_type in location_type_dict:
                record['location_type_id'] = location_type_dict[location_type]
            else:
                record['location_type_id'] = civicrm.getLocationTypeID(location_type)
                location_type_dict[location_type] = record['location_type_id']
            if not record['location_type_id']:
                civicrm.log("Could not write contact phone number, location type %s could not be resolved" % location_type,
                    logging.WARN, 'importer', 'import_contact_phone', 'Phone', None, None, time.time()-timestamp)
                continue

        # get phone-type-id
        if 'phone_type_id' not in record:
            phone_type = record.get('phone_type', parameters.get('phone_type', 'Phone'))

            option_group_id = civicrm.getOptionGroupID('phone_type')
            phone_type_id = civicrm.getOptionValue(option_group_id, phone_type)

            if not phone_type_id:
                civicrm.log("Could not write contact phone number, phone type %s could not be resolved" % phone_type,
                    logging.WARN, 'importer', 'import_contact_phone', 'Phone', None, None, time.time()-timestamp)
                continue
            else:
                record['phone_type_id'] = phone_type_id

        if multiple=='allow':
            # allow multiple phone numbers of the same type
            phone_numbers = civicrm.getPhoneNumbers(record['contact_id'], record['location_type_id'])
            # check if it is already there...
            already_there = False
            for phone_number in phone_numbers:
                if record['phone'].lower() == phone_number.get('phone').lower():
                    # it is already there
                    civicrm.log("No new phone numbers for contact [%s]" % str(phone_number.get('contact_id')),
                        logging.INFO, 'importer', 'import_contact_phone', 'Phone', phone_number.get('id'), phone_number.get('contact_id'), time.time()-timestamp)
                    already_there = True
                    break

            # nothing? then create new phone_number record
            if not already_there:
                phone_number = civicrm.createPhoneNumber(record)
                civicrm.log("Added new phone_number for contact [%s]" % str(phone_number.get('contact_id')),
                    logging.INFO, 'importer', 'import_contact_phone', 'Phone', phone_number.get('id'), phone_number.get('contact_id'), time.time()-timestamp)
        else:
            try:
                phone_number = civicrm.getPhoneNumber(record)
            except:
                phone_number = None
                civicrm.logException("Exception while updating phone number for [%s]. Data was %s, exception: " % (record['contact_id'], str(record)),
                    logging.ERROR, 'importer', 'import_contact_phone', 'Phone', None, record['contact_id'], time.time()-timestamp)

            if not no_update and phone_number:
                if 'location_type' in record:
                    del record['location_type']
                if 'location_type_id' in record:
                    del record['location_type_id']
                if 'external_identifier' in record:
                    del record['external_identifier']

                changed = phone_number.update(record, store=True)
                if changed:
                    civicrm.log("Updated phone number: %s" % str(phone_number),
                        logging.INFO, 'importer', 'import_contact_phone', 'Phone', phone_number.get('id'), record['contact_id'], time.time()-timestamp)
                else:
                    civicrm.log("Nothing changed for phone number: %s" % str(phone_number),
                        logging.INFO, 'importer', 'import_contact_phone', 'Phone', phone_number.get('id'), record['contact_id'], time.time()-timestamp)

            elif no_update and phone_number:
                civicrm.log("Phone_number exists and was not updated: %s" % str(phone_number),
                    logging.INFO, 'importer', 'import_contact_phone', 'Phone', phone_number.get('id'), record['contact_id'], time.time()-timestamp)

            else:
                phone_number = civicrm.createPhoneNumber(record)
                civicrm.log("Added new phone_number for contact [%s]" % str(phone_number.get('contact_id')),
                    logging.INFO, 'importer', 'import_contact_phone', 'Phone', phone_number.get('id'), phone_number.get('contact_id'), time.time()-timestamp)


def import_contact_prefix(civicrm, record_source, parameters=dict()):
    """
    Imports contact prefixes

    Expects the fields: "prefix" and identification ('id', 'external_identifier', 'contact_id')

    if parameters['no_update'] is True we do not overwrite existing prefixes
    """
    no_update = parameters.get('no_update', False)
    for record in record_source:
        timestamp = time.time()
        contact_id = civicrm.getContactID(record)
        if not contact_id:
            civicrm.log("Could not find contact ID in record.",
              logging.WARN, 'importer', 'import_contact_prefix', 'Contact', None, None, time.time()-timestamp)
            continue

        contact = civicrm.getEntity('Contact', {'id': contact_id})
        if not contact:
            civicrm.log("Could not find contact with external id '%s'" % record['external_identifier'],
              logging.WARN, 'importer', 'import_contact_prefix', 'Contact', None, None, time.time()-timestamp)
        else:
            if not record.get('prefix_id', None):
                prefix = record.get('prefix', None)
                prefix_id = civicrm.getOptionValue(civicrm.getOptionGroupID('individual_prefix'), prefix)
                if not prefix_id:
                    civicrm.log("Prefix '%s' doesn't exist!" % prefix,
                      logging.WARN, 'importer', 'import_contact_prefix', 'Contact', None, None, time.time()-timestamp)
                    continue
                else:
                    record['prefix_id'] = prefix_id
                    del record['prefix']

            if no_update:
                changed = contact.fill(record, True)
            else:
                changed = contact.update(record, True)
            if changed:
                civicrm.log("Updated Prefix for '%s'" % str(str(contact), 'utf8'),
                  logging.INFO, 'importer', 'import_contact_prefix', 'Contact', contact.get('id'), None, time.time()-timestamp)
            elif no_update:
                civicrm.log("Prefix for '%s' already exists and was not updated." % str(str(contact), 'utf8'),
                  logging.INFO, 'importer', 'import_contact_prefix', 'Contact', contact.get('id'), None, time.time()-timestamp)
            else:
                civicrm.log("Prefix for '%s' was up to date." % str(str(contact), 'utf8'),
                  logging.INFO, 'importer', 'import_contact_prefix', 'Contact', contact.get('id'), None, time.time()-timestamp)



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
            civicrm.log("Could not write contact greeting, contact not found for '%s'" % str(str(contact), 'utf8'),
                logging.WARN, 'importer', 'import_contact_greeting', 'Contact', None, None, time.time()-timestamp)
            continue

        update = dict()

        for key in ['postal_greeting', 'email_greeting']:
            if record.get(key, None):
                key_id = civicrm.getOptionValue(
                    civicrm.getOptionGroupID(key),
                    record[key]
                    )
                if key_id:
                    update[key + '_id'] = key_id

            if record.get(key + '_custom', None):
                update[key + '_custom'] = record[key + '_custom']

        changed = contact.update(update, True)
        if changed:
            civicrm.log("Updated greeting settings for contact: %s" % str(str(contact), 'utf8'),
                logging.INFO, 'importer', 'import_contact_greeting', 'Contact', contact.get('id'), None, time.time()-timestamp)
        else:
            civicrm.log("Greeting settings not changed for contact: %s" % str(str(contact), 'utf8'),
                logging.INFO, 'importer', 'import_contact_greeting', 'Contact', contact.get('id'), None, time.time()-timestamp)


def import_contact_email(civicrm, record_source, parameters=dict()):
    """
    Imports contact email address

    Expects the fields:
    "email", "location_type"
    and identification ('id', 'external_identifier', 'contact_id')

    parameters:
     multiple:    'allow' - allows multiple emails per type
     no_update: if True, existing email-addresses won't be overwritten; default is False
    """
    _prepare_parameters(parameters)
    no_update = parameters.get('no_update', False)
    multiple = parameters.get('multiple', False)
    for record in record_source:
        timestamp = time.time()
        record['contact_id'] = civicrm.getContactID(record)
        if not record['contact_id']:
            civicrm.log("Could not write contact email, contact not found for '%s'" % str(record),
                logging.WARN, 'importer', 'import_contact_email', 'Email', None, None, time.time()-timestamp)
            continue

        # get the location type id
        if ('location_type_id' not in record):
            location_type = record.get('location_type', parameters.get('location_type', 'Main'))
            location_type_dict = _get_or_create_from_params('location_type_dict', parameters)

            if location_type in location_type_dict:
                record['location_type_id'] = location_type_dict[location_type]
            else:
                record['location_type_id'] = civicrm.getLocationTypeID(location_type)
                location_type_dict[location_type] = record['location_type_id']
            if not record['location_type_id']:
                civicrm.log("Could not write contact email, location type %s could not be resolved" % location_type,
                    logging.WARN, 'importer', 'import_contact_email', 'Email', None, None, time.time()-timestamp)
                continue

        if multiple=='allow':
            # allow multiple emails of the same type
            emails = civicrm.getEmails(record['contact_id'], record['location_type_id'])
            # check if it is already there...
            already_there = False
            for email in emails:
                if record['email'].lower() == email.get('email').lower():
                    # it is already there
                    civicrm.log("No new emails for contact [%s]" % str(email.get('contact_id')),
                        logging.INFO, 'importer', 'import_contact_email', 'Email', email.get('id'), email.get('contact_id'), time.time()-timestamp)
                    already_there = True
                    break

            # nothing? then create new email record
            if not already_there:
                email = civicrm.createEmail(record['contact_id'], record['location_type_id'], record['email'])
                civicrm.log("Added new email for contact [%s]" % str(email.get('contact_id')),
                    logging.INFO, 'importer', 'import_contact_email', 'Email', email.get('id'), email.get('contact_id'), time.time()-timestamp)

        else:
            # find and update/replace the email with the given contact
            email = civicrm.getEmail(record['contact_id'], record['location_type_id'])
            if not no_update and email:
                if 'location_type' in record:
                    del record['location_type']
                if 'location_type_id' in record:
                    del record['location_type_id']
                if 'external_identifier' in record:
                    del record['external_identifier']
                changed = email.update(record, store=True)
                if changed:
                    civicrm.log("Updated email address: %s" % str(email),
                        logging.INFO, 'importer', 'import_contact_email', 'Email', email.get('id'), record['contact_id'], time.time()-timestamp)
                else:
                    civicrm.log("Nothing changed for email: %s" % str(email),
                        logging.INFO, 'importer', 'import_contact_email', 'Email', email.get('id'), record['contact_id'], time.time()-timestamp)

            elif no_update and email:
                civicrm.log("Email exists and was not updated: %s" % str(email),
                    logging.INFO, 'importer', 'import_contact_phone', 'Phone', email.get('id'), record['contact_id'], time.time()-timestamp)

            else:
                email = civicrm.createEmail(record['contact_id'], record['location_type_id'], record['email'])
                civicrm.log("Created email address: %s" % str(email),
                    logging.INFO, 'importer', 'import_contact_email', 'Email', email.get('id'), record['contact_id'], time.time()-timestamp)


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
    membership_primary_attributes=['contact_id']
    if 'multiple' in parameters and parameters['multiple']:
        # multiple means, that we allow multiple membership types per contact
        membership_primary_attributes.append('membership_type_id')
        membership_primary_attributes.append('membership_type')

    for record in record_source:
        timestamp = time.time()
        record['contact_id'] = civicrm.getContactID(record)
        if not record['contact_id']:
            civicrm.log("Could not write membership, contact not found for '%s'" % str(record),
                logging.WARN, 'importer', 'import_membership', 'Membership', None, None, time.time()-timestamp)
            continue

        record['is_override'] = 1     # write status as-is
        if 'status' in record:
            status_id = civicrm.getMembershipStatusID(record['status'])
            if not status_id:
                civicrm.log("Membership status '%s' does not exist!" % record['status'],
                    logging.WARN, 'importer', 'import_membership', 'Membership', None, None, time.time()-timestamp)
                continue

            record['status_id'] = status_id
            del record['status']

        try:
            membership = civicrm.createOrUpdate('Membership', record, update_type='update', primary_attributes=membership_primary_attributes)
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
        if group_ids==None:    # GET THE TAG IDS!
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
    Set set of tags for a contact
    """
    parameters['entity_type'] = 'Contact'
    parameters['entity_table'] = 'civicrm_contact'
    parameters['key_fields'] = ['id', 'external_identifier']
    return import_entity_tags(civicrm, record_source, parameters)


def import_entity_tags(civicrm, record_source, parameters=dict()):
    """
    (Un)set a set of tags for entities
    """
    _prepare_parameters(parameters)

    entity_type = parameters.get('entity_type', None)
    if not entity_type:
        civicrm.log("Could not (un)tag entity, no entity_type given",
            logging.WARN, 'importer', 'import_entity_tags', 'EntityTag', None, None, time.time()-timestamp)
        return

    entity_table = parameters.get('entity_table', None)
    if not entity_table:
        civicrm.log("Could not (un)tag entity, no entity_table given",
            logging.WARN, 'importer', 'import_entity_tags', 'EntityTag', None, None, time.time()-timestamp)
        return

    key_fields = parameters.get('key_fields', ['id', 'external_identifier'])


    for record in record_source:
        if entity_type=='Contact':
            entity_id = civicrm.getContactID(record)
            if not entity_id:
                civicrm.log("Contact not found: ID %s" % entity_id,
                    logging.WARN, 'importer', 'import_contact_tags', 'Contact', entity_id, None, 0)
                continue
        else:
            entity_id = civicrm.getEntityID(record, entity_type, key_fields)


        tag_ids = parameters.get('tag_ids', None)
        allFound = False
        if not tag_ids==None:
            # find the tags
            allFound = True
            for tag_name in record.keys():
                allFound &= (tag_name in tag_ids)

        if not allFound:    # GET THE TAG IDS!
            parameters_lock = parameters['lock']
            parameters_lock.acquire()
            # test again, maybe another thread already created them...
            tag_ids = parameters.get('tag_ids', dict())
            parameters['tag_ids'] = tag_ids
            for tag_name in record.keys():
                tag_id = civicrm.getOrCreateTagID(tag_name)
                tag_ids[tag_name] = tag_id
                civicrm.log("Tag '%s' has ID %s" % (tag_name, tag_id),
                    logging.INFO, 'importer', 'import_entity_tags', 'EntityTag', tag_id, None, 0)

            parameters_lock.notifyAll()
            parameters_lock.release()

        if entity_type=='Contact':
            currentTags = civicrm.getContactTagIds(entity_id)
        else:
            currentTags = civicrm.getEntityTagIds(entity_id, entity_table)

        tags2change = dict()
        for tag_name in record.keys():
            if not (tag_name in key_fields):
                desiredState = (record[tag_name].lower() in ['true', 1, '1', 'x', 'yes', 'y', 'ja', 'j'])
                currentState = (tag_ids[tag_name] in currentTags)
                if currentState != desiredState:
                    tags2change[tag_ids[tag_name]] = desiredState
        if tags2change:
            civicrm.log("Modifying tags for %s [%s]" % (entity_type, entity_id),
                logging.INFO, 'importer', 'import_entity_tags', 'EntityTag', entity_id, None, 0)
            for tag_id in tags2change:
                if entity_type=='Contact':
                    civicrm.tagContact(entity_id, tag_id, tags2change[tag_id])
                else:
                    civicrm.tagEntity(entity_id, entity_table, tag_id, tags2change[tag_id])
        else:
            civicrm.log("Tags are up to date for %s [%s]" % (entity_type, entity_id),
                logging.INFO, 'importer', 'import_entity_tags', 'EntityTag', entity_id, None, 0)


def import_delete_entity(civicrm, record_source, parameters=dict()):
    """
    Will delete the given entity if identified

    Parameters:
        'entity_type'    - the entity type, default is Contact
        'identifiers'     - a list of column names that will be used to uniquely identify the
                            entity. Only attributes provided by the datasource will be taken into account
        'silent'         - if True, don't log error if not found
    """
    _prepare_parameters(parameters)
    timestamp = time.time()
    entity_type = parameters.get('entity_type', 'Contact')
    identifiers = list(parameters.get('identifiers', ['id', 'external_identifier']))
    silent = parameters.get('silent', False)

    for record in record_source:
        # lookup contact_id
        for external_identifier in ['contact_external_identifier', 'external_identifier']:
            if external_identifier in record:
                if record[external_identifier]:
                    record['contact_id'] = civicrm.getContactID({'external_identifier': record[external_identifier]})
                del record[external_identifier]
                if external_identifier in identifiers:
                    identifiers.remove(external_identifier)
                    identifiers.append('contact_id')
                if not record['contact_id']:
                    civicrm.log("Couldn't find or identify related contact!",
                        logging.WARN, 'importer', 'import_delete_entity', entity_type, None, None, time.time()-timestamp)
                    continue

        # lookup location_type
        if 'location_type' in record and 'location_type_id' not in record:
            record['location_type_id'] = civicrm.getLocationTypeID(record['location_type'])
            del record['location_type']
            if 'location_type' in identifiers:
                identifiers.remove('location_type')
                identifiers.append('location_type_id')

        # first, find the entity
        #print record
        #print identifiers
        entity = civicrm.getEntity(entity_type, record, primary_attributes=identifiers)

        #if entity_type is Contact and entity wasn't found we need a second
        #lookup for deleted Contacts (dirty hack)
        if entity_type == 'Contact' and not entity:
            record['is_deleted'] = 1
            identifiers.append('is_deleted')
            entity = civicrm.getEntity(entity_type, record, primary_attributes=identifiers)

        if entity:
            entity.delete()
            civicrm.log("%s [%s] deleted." % (entity_type, entity.get('id')),
                logging.INFO, 'importer', 'import_delete_entity', entity_type, None, None, time.time()-timestamp)
        elif not silent:
            civicrm.log("Couldn't find or identify entity to delete!",
                logging.WARN, 'importer', 'import_delete_entity', entity_type, None, None, time.time()-timestamp)



def parallelize(civicrm, import_function, workers, record_source, parameters=dict()):
    _prepare_parameters(parameters)
    # if only on worker, just call directly
    if workers==1:
        for record in record_source:
            try:
                timestamp = time.time()
                import_function(civicrm, [record], parameters)
            except:
                civicrm.logException("Exception caught for '%s' on procedure '%s'. Exception was: " % (threading.currentThread().name, import_function.__name__),
                    logging.ERROR, 'importer', import_function.__name__, None, None, None, time.time()-timestamp)
                civicrm.log("Failed record was: %s" % str(record),
                    logging.ERROR, 'importer', import_function.__name__, None, None, None, time.time()-timestamp)
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
            record_list.append(next(record_source_iterator))
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
            self.throttle = 0.1
            self.start()

        def run(self):
            active = True
            while active:
                if self.throttle > 0:
                    time.sleep(self.throttle)

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
                        timestamp = time.time()
                        self.function(self.civicrm, [record], self.parameters)
                    except:
                        civicrm.logException("Exception caught for '%s' on procedure '%s'. Exception was: " % (threading.currentThread().name, import_function.__name__),
                            logging.ERROR, 'importer', import_function.__name__, None, None, None, time.time()-timestamp)
                        civicrm.log("Failed record was: %s" % str(record),
                            logging.ERROR, 'importer', import_function.__name__, None, None, None, time.time()-timestamp)



    for i in range(workers):
        thread_list.append(Worker(import_function, civicrm, parameters, record_list, record_list_lock))

    # finally, feed the queue
    remaining_records = True
    while remaining_records:
        record_list_lock.acquire()
        record_list_lock.wait()
        try:
            record_list.append(next(record_source_iterator))
        except:
            remaining_records = False
        record_list_lock.release()

    for worker in thread_list:
        if worker.isAlive():
            worker.join()

    civicrm.log("Parallelized procedure '%s' completed." % import_function.__name__,
        logging.INFO, 'importer', 'parallelize', None, None, None, time.time()-timestamp)
