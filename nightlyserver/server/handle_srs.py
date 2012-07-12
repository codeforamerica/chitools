import json
import iso8601
from db_info import *

def save_sr_data(sr, db):
    sr = clean_document(sr)
    sr['EID'] = sr['srs-EID']
    case_id = db[COLLECTION_CASE_INDEX].find_one({'EID': sr['srs-EID']})
    if not case_id:
        if sr['srs-CREATION_REASON_CODE'] == 'FOLLOW_O':
            # for follow-ons, find the parent
            parent_case_id = db[COLLECTION_CASE_INDEX].find_one({'EID': sr['srs-ORIG_SERVICE_REQUEST_EID']})
            if parent_case_id:
                # search cases or orphans
                case_id_str = parent_case_id['case']
                orphan = False
                case_data = db[COLLECTION_CASES].find_one({'_id': case_id_str})
                if not case_data:
                    case_data = db[COLLECTION_ORPHANS].find_one({'_id': case_id_str})
                    orphan = True
                    if not case_data:
                        raise Exception('Indexed case (%s) could not be found' % json.dumps(parent_case_id))
                
                # add to case and index
                case_data['requests'].append(sr)
                
                # update metadata on case if it's not an orphan
                if not orphan:
                    update_case_metadata(case_data)
                
                collection = db[orphan and COLLECTION_ORPHANS or COLLECTION_CASES]
                collection.save(case_data)
                db[COLLECTION_CASE_INDEX].insert({
                    '_id': sr['srs-SERVICE_REQUEST_NUM'],
                    'EID': sr['srs-EID'],
                    'case': case_id_str,
                })
                
            else:
                # insert as orphan
                case_data = {
                    '_id': sr['srs-SERVICE_REQUEST_NUM'],
                    'EID': sr['srs-EID'],
                    'requests': [sr],
                    'duplicates': []
                }
                case_id_str = db[COLLECTION_ORPHANS].insert(case_data)
                # insert into index
                db[COLLECTION_CASE_INDEX].insert({
                    '_id': sr['srs-SERVICE_REQUEST_NUM'],
                    'EID': sr['srs-EID'],
                    'case': case_id_str,
                })
                # insert parent into index
                db[COLLECTION_CASE_INDEX].insert({
                    '_id': str(sr['srs-ORIG_SERVICE_REQUEST_EID']),
                    'EID': sr['srs-ORIG_SERVICE_REQUEST_EID'],
                    'case': case_id_str,
                })
                
        elif 'DUP' in sr['srs-STATUS_CODE']:
            # Duplicate request
            # TODO: implement this? (don't drop duplicates)
            pass
            
        else:
            # for root SRs that are not already indexed, create a new case
            case_data = {
                '_id': sr['srs-SERVICE_REQUEST_NUM'],
                'EID': sr['srs-EID'],
                'requests': [sr],
                'duplicates': []
            }
            update_case_metadata(case_data)
            # since it's the root, it's not orphaned
            case_id_str = db[COLLECTION_CASES].insert(case_data)
            # insert into index
            db[COLLECTION_CASE_INDEX].insert({
                '_id': sr['srs-SERVICE_REQUEST_NUM'],
                'EID': sr['srs-EID'],
                'case': case_id_str,
            })
        
    else:
        # has a case_id
        case_id_str = case_id['case']
        # look for a proper case
        orphan = False
        case_data = db[COLLECTION_CASES].find_one({'_id': case_id_str})
        if not case_data:
            case_data = db[COLLECTION_ORPHANS].find_one({'_id': case_id_str})
            orphan = True
            if not case_data:
                raise Exception('Indexed case (%s) could not be found' % json.dumps(case_id))
        
        if sr['srs-CREATION_REASON_CODE'] == 'FOLLOW_O':
            # follow-on
            # if in case
            sr_index = find_sr_in_list(sr, case_data['requests'])
            if sr_index > -1:
                case_data['requests'][sr_index] = sr
            else:
                # add to case and sort requests by date (put in front of requests list?)
                case_data['requests'].insert(0, sr)
            
            # if parent has case
            parent_case_id = db[COLLECTION_CASE_INDEX].find_one({'EID': sr['srs-ORIG_SERVICE_REQUEST_EID']})
            if parent_case_id and parent_case_id['case'] != case_id_str:
                parent_case_id_str = parent_case_id['case']
                parent_orphan = False
                parent_case_data = db[COLLECTION_CASES].find_one({'_id': parent_case_id_str})
                if not parent_case_data:
                    parent_orphan = True
                    parent_case_data = db[COLLECTION_ORPHANS].find_one({'_id': parent_case_id_str})
                    if not parent_case_data:
                        raise Exception('Indexed case (%s) could not be found' % json.dumps(parent_case_id))
                # add known case requests to parent case
                parent_case_data['requests'].extend(case_data['requests'])
                # update metadata on case if it's not an orphan
                if not parent_orphan:
                    update_case_metadata(parent_case_data)
                # save parent
                db[parent_orphan and COLLECTION_ORPHANS or COLLECTION_CASES].save(parent_case_data)
                # update indices
                for subrequest in case_data['requests']:
                    db[COLLECTION_CASE_INDEX].update({'EID': subrequest['srs-EID']}, {'$set': {'case': parent_case_id_str}})
                # remove known case
                db[orphan and COLLECTION_ORPHANS or COLLECTION_CASES].remove(case_data['_id'])
                
            else:
                # update metadata on case if it's not an orphan
                if not orphan:
                    update_case_metadata(case_data)
                # save case
                db[orphan and COLLECTION_ORPHANS or COLLECTION_CASES].save(case_data)
                # index the parent for this case
                db[COLLECTION_CASE_INDEX].insert({
                    '_id': str(sr['srs-ORIG_SERVICE_REQUEST_EID']),
                    'EID': sr['srs-ORIG_SERVICE_REQUEST_EID'],
                    'case': case_id_str,
                })
        
        elif 'DUP' in sr['srs-STATUS_CODE']:
            # Duplicate request
            # Should this not be a failure?
            raise Exception('Duplicate case (%s) should not have been indexed by a follow-on' % json.dumps(case_id))
        
        else:
            # A root that was indexed by a follow-on
            if orphan:
                # create new case
                parent_case_data = {
                    '_id': sr['srs-SERVICE_REQUEST_NUM'],
                    'EID': sr['srs-EID'],
                    'requests': [sr],
                    'duplicates': []
                }
                # add requests from orphan case
                parent_case_data['requests'].extend(case_data['requests'])
                # update metadata on case
                update_case_metadata(parent_case_data)
                # insert new case
                parent_case_id_str = db[COLLECTION_CASES].save(parent_case_data)
                # update indices (need to update ALL, not just the orphan case's, since we already matched this one)
                for subrequest in parent_case_data['requests']:
                    db[COLLECTION_CASE_INDEX].update({'EID': subrequest['srs-EID']}, {'$set': {'case': parent_case_id_str}})
                #remove orphan
                db[COLLECTION_ORPHANS].remove(case_data['_id'])
                    
            # if there is already a real case
            else:
                # update it
                sr_index = find_sr_in_list(sr, case_data['requests'])
                # in theory we should always pass this condition...
                if sr_index > -1:
                    case_data['requests'][sr_index] = sr
                else:
                    # add to case and sort requests by date (put in front of requests list?)
                    case_data['requests'].insert(0, sr)
                    
                # update metadata on case
                update_case_metadata(case_data)
                db[COLLECTION_CASES].save(case_data)


def update_case_metadata(sr_case):
    first = sr_case['requests'][0]
    last = sr_case['requests'][len(sr_case['requests']) - 1]
    # Wrap in a try..except for cases where we have dates lacking timezones and can't compare :\
    # (Not really worth the research ATM to see how to fix them up.)
    try:
        for sr in sr_case['requests']:
            if sr['srs-CREATED_DATE'] > last['srs-CREATED_DATE']:
                last = sr
    except:
        pass
    sr_case['service_code'] = first['srs-TYPE_CODE'];
    sr_case['requested_datetime'] = first['srs-CREATED_DATE'];
    sr_case['updated_datetime'] = last['srs-UPDATED_DATE'];
    sr_case['priority'] = first['srs-PRIORITY_CODE'];
    sr_case['location'] = [first['srs-X_COORDINATE'], first['srs-Y_COORDINATE']];
    # A case is open if any SRs in it are open (since some follow-ons are branching, this matters)
    open_srs = filter(lambda sr: sr['srs-STATUS_CODE'].startswith('O'), sr_case['requests'])
    sr_case['status'] = len(open_srs) > 0 and 'open' or 'closed'


def find_sr_in_list(sr, sr_list):
    sr_id = sr['srs-SERVICE_REQUEST_NUM']
    for index, item in enumerate(sr_list):
        if item['srs-SERVICE_REQUEST_NUM'] == sr_id:
            return index
    return -1
    

def clean_document(document):
    cleaned = {}
    for k, v in document.iteritems():
        new_k = k.replace('.', '-')
        # ISO 8601 dates will be demarcated by "date::[date]"
        if isinstance(v, basestring) and v.startswith('date::'):
            try:
                v = iso8601.parse_date(v[6:])
            except:
                pass
        
        elif isinstance(v, dict):
            v = clean_document(v)
        
        elif isinstance(v, list):
            new_v = []
            for item in v:
                if isinstance(item, dict):
                    item = clean_document(item)
                new_v.append(item)
            v = new_v
                
        cleaned[new_k] = v
        
    return cleaned


def save_sr_type_data(types, db):
    '''Save/update SR code/name info in DB.
    Takes a dictionary: {code: name}
    or a list: [{code: code, name: name}]'''
    
    if isinstance(types, dict):
        for code, name in types.iteritems():
            insert_sr_type({
                'code': code,
                'name': name
            }, db)
    else:
        for document in types:
            insert_sr_type(document, db)


def insert_sr_type(type_info, db):
    type_info['_id'] = type_info['code']
    db[COLLECTION_SERVICES].update({'_id': type_info['code']}, type_info, True)
