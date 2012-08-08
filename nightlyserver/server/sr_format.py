'''Tools for formatting service requests pulled from the database into Open311 SRs.'''

from db_info import *

def format_address(sr, regional=False):
    '''Returns a nicely formatted address for a service request.'''
    # TODO: fix capitalization?
    address = str(sr['srs-STREET_NUMBER'])
    if sr['srs-STREET_NAME_PREFIX']:
        address = '%s %s' % (address, sr['srs-STREET_NAME_PREFIX'])
    address = '%s %s' % (address, sr['srs-STREET_NAME'])
    if sr['srs-STREET_SUFFIX_DIRECTION']:
        address = '%s %s' % (address, sr['srs-STREET_SUFFIX_DIRECTION'])
    if sr['srs-STREET_NAME_SUFFIX']:
        address = '%s %s' % (address, sr['srs-STREET_NAME_SUFFIX'])
    if regional:
        address = '%s, %s, %s %s' % (address, sr['srs-CITY'], sr['srs-STATE_CODE'], sr['srs-ZIP_CODE'])
    return address


def format_case(sr_case, db, legacy=False):
    '''Format a case as an Open311 Service Request'''
    
    # create the notes list
    notes = notes_for_case(sr_case, db)
    
    # is the whole case closed?
    last_sr = sr_case['requests'][-1]
    overall_status = last_sr['srs-STATUS_CODE'].startswith('O-') and 'open' or 'closed'
    
    if overall_status == 'closed':
        # add an activity for closing the case
        notes.append({
            'datetime': last_sr['srs-UPDATED_DATE'],
            'description': 'Service request completed.',
            'type': 'closed'
        })
    
    # set detailed status information to be the latest activity
    status_notes = len(notes) and notes[-1]['description'] or None
    
    base_sr = sr_case['requests'][0]
    
    sr = {
        'service_request_id': base_sr['srs-SERVICE_REQUEST_NUM'],
        'agency_responsible': base_sr['codes_group-DESCRIPTION'],
        'status': overall_status,
        'status_notes': status_notes,
        'service_name': get_service_by_code(base_sr['srs-TYPE_CODE'], db),
        'service_code': get_service_uuid_by_code(base_sr['srs-TYPE_CODE'], db),
        'description': base_sr['srs-DETAILS'],
        'requested_datetime': base_sr['srs-CREATED_DATE'],
        'updated_datetime': last_sr['srs-UPDATED_DATE'],
        'address': format_address(base_sr, regional=True),
        'zipcode': base_sr['srs-ZIP_CODE'],
        'lat': base_sr['srs-Y_COORDINATE'],
        'long': base_sr['srs-X_COORDINATE'],
        
        # CUSTOM
        'notes': notes,
        'extended_attributes': {
            'channel': base_sr['srs-METHOD_RECEIVED_CODE'],
            # Not sure this is actually ward
            'ward': base_sr['srs-GEO_AREA_VALUE'],
        },
        
        # Intentionally not filled in
        'media_url': None, # New data should have this (e.g. coming in through the API), but old data usually won't
        'service_notice': None, # not really clear what would go here
        'expected_datetime': None, # don't have the necessary info
        'address_id': None, # not sure there's even anything like this in CSR
    }
    
    # old (non-CB) style
    if legacy:
        sr['activities'] = notes_for_case(sr_case, db, legacy=True)
        sr['received_via'] = base_sr['srs-METHOD_RECEIVED_CODE']
    
    return sr


def notes_for_case(sr_case, db, legacy=False):
    '''Generate a list of notes based on CSR activities and follow-ons'''
    
    notes = []
    for index, subrequest in enumerate(sr_case['requests']):
        if index > 0:
            # create an activity to represent a follow-on
            note = {
                'datetime': subrequest['srs-CREATED_DATE'],
                'description': get_service_by_code(subrequest['srs-TYPE_CODE'], db) or subrequest['srs-TYPE_CODE'],
                'type': 'subrequest',
                'properties': {
                    'service_request_id': subrequest['srs-SERVICE_REQUEST_NUM'],
                    'service_code': subrequest['srs-TYPE_CODE'],
                    'agency_responsible': subrequest['codes_group-DESCRIPTION'],
                    # TODO: should this carry more info?
                    'details': subrequest['srs-DETAILS'],
                }
            }
            notes.append(note)
        
        # Create an activity for all completed activities on the SR
        # NOTE: Many activities are auto-created but are never done,
        # which is why we only show completed activities.
        # TODO: include assigned activities? Need to research it.
        for sr_activity in subrequest['activities']:
            if sr_activity['act-COMPLETE_DATE']:
                note = {
                    'datetime': sr_activity['act-COMPLETE_DATE'],
                    'description': sr_activity['codes_act-DESCRIPTION'],
                    'type': 'activity',
                    'properties': {}
                }
                # including details for now
                if 'act-DETAILS' in sr_activity and sr_activity['act-DETAILS']:
                    note['properties']['details'] = sr_activity['act-DETAILS']
                notes.append(note)
    
    # ensure activities are in chronological order
    notes.sort(key=lambda note: note['datetime'])
    return notes


def get_service_by_code(code, db):
    """Get the service name associated with a service code."""
    # FIXME: this should probably cache the list of services instead of hitting the DB
    service = db[COLLECTION_SERVICES].find_one({'_id': code})
    return service and service['name'] or None


def get_service_uuid_by_code(code, db):
    """Get the service name associated with a service code."""
    # FIXME: this should probably cache the list of services instead of hitting the DB
    service = db[COLLECTION_SERVICES].find_one({'_id': code})
    return service and service['uuid'] or None
    