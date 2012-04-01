"""
Tools for transforming 311 data from the Chicago data portal into something
searchable and useful inside a MongoDB database.

Customize the constants at the top of this file to change the DB and 
collections that will be inserted into.

Calling with the --all argument will do all operations. If a file path
is included after --all, it will parse and import a JSON document
(if not included, this part will be skipped).

Does the following things:
-Parse JSON data from the data portal and insert it into Mongo (DB_BASIC_COLLECTION)
-Combine multiple rows that all are about the same request into one (DB_COMBINED_COLLECTION)
-Extract the availbale service types from the requests (DB_SERVICES_COLLECTION)
-Combine requests and their follow-ons into "Cases" (DB_CASES_COLLECTION)

NOTE:
IF IMPORTING DATA FROM A JSON FILE, YOU SHOULD USE THE -u OPTION:
python -u --file filename.json
THIS WILL TURN OFF OUTPUT BUFFERING SO YOU CAN ACTUALLY SEE PROGRESS ;)
"""

import json
import pymongo
import iso8601
from time import sleep
from optparse import OptionParser

DB_HOST = 'localhost'
DB_PORT = 27017
DB_NAME = 'TestData'
DB_BASIC_COLLECTION = 'ServiceRequests'
DB_SERVICES_COLLECTION = 'Services'
DB_COMBINED_COLLECTION = 'ServiceRequestsCombined'
DB_CASES_COLLECTION = 'Cases'

# Connect to MONGOMONGOMONGO
connection = pymongo.Connection(DB_HOST, DB_PORT)
db = connection[DB_NAME]

##################### UTILITIES ######################

def earliest(a, b):
  if a and b:
    return a < b and a or b
  else:
    return a or b

def latest(a, b):
  if a and b:
    return a > b and a or b
  else:
    return a or b


################# COLLECTION SETUP ##################

def setup_basic_collection():
  db[DB_BASIC_COLLECTION].ensure_index('service_request_number')
  db[DB_BASIC_COLLECTION].ensure_index('created')
  db[DB_BASIC_COLLECTION].ensure_index('updated')
  db[DB_BASIC_COLLECTION].ensure_index('completed')
  db[DB_BASIC_COLLECTION].ensure_index('service_request_type')

def setup_combined_collection():
  db[DB_COMBINED_COLLECTION].ensure_index('service_request_number')
  db[DB_COMBINED_COLLECTION].ensure_index('created')
  db[DB_COMBINED_COLLECTION].ensure_index('updated')
  db[DB_COMBINED_COLLECTION].ensure_index('completed')
  db[DB_COMBINED_COLLECTION].ensure_index('service_request_type')

def setup_cases_collection():
  db[DB_CASES_COLLECTION].ensure_index('initial_request')
  db[DB_CASES_COLLECTION].ensure_index('initial_type')
  db[DB_CASES_COLLECTION].ensure_index('created')

def setup_services_collection():
  db[DB_SERVICES_COLLECTION].ensure_index('service_name')
  db[DB_SERVICES_COLLECTION].ensure_index('service_code')


#################### ACTIONS ########################    

def load_srs_from_file(filename):
  print 'Reading %s...' % filename
  thefile = open(filename)
  entries = json.loads(thefile.read())
  thefile.close()
  columns = entries['meta']['view']['columns']
  chunk_size = len(entries['data']) / 100
  print 'Importing data in chunks of %s...' % chunk_size
  chunked_data = []
  for index, entry in enumerate(entries['data']):
    chunked_data.append(make_sr_document(entry, columns))
    
    # db[DB_BASIC_COLLECTION].insert(make_sr_document(entry, columns))
    if index > 0 and index % chunk_size == 0:
      db[DB_BASIC_COLLECTION].insert(chunked_data)
      chunked_data = []
      # Need a sleep so we don't pretty much halt the system
      sleep(2)
      if (index / chunk_size) % 10 == 0:
        print '%s%%' % (index / chunk_size),
      else:
        print '.',


def make_sr_document(sr_data, columns):
  doc = { 'meta': {} }
  for index, column in enumerate(columns):
    if column['dataTypeName'] == 'meta_data':
      doc['meta'][column['name']] = sr_data[index]
    else:
      doc[column['name']] = sr_data[index]
  
  # add 2D index if lat/long coordinates are present
  if doc['latitude']:
    doc['location'] = [float(doc['latitude']), float(doc['longitude'])]
  
  # convert dates to actual dates
  doc['created'] = iso8601.parse_date(doc['creation_date'])
  del doc['creation_date']
  doc['updated'] = iso8601.parse_date(doc['updated_on'])
  del doc['updated_on']
  if doc['completion_date']:
    doc['completed'] = iso8601.parse_date(doc['completion_date'])
    del doc['completion_date']
  
  # status should be all lower case
  doc['status'] = doc['status'].lower()
  
  # convert "N/A" follow-ons to None
  if doc['follow_on_service_request'].lower() == 'n/a':
    doc['follow_on_service_request'] = None
  
  # TODO: should set a service_request_code based on service_request_type

  return doc


def extract_services():
  type_names = db[DB_BASIC_COLLECTION].distinct('service_request_type')
  latest_code = db[DB_SERVICES_COLLECTION].find().count()
  type_docs = []
  for name in type_names:
    if db[DB_SERVICES_COLLECTION].find_one({'service_name': name}) == None:
      type_docs.append({
        'service_code': latest_code,
        'service_name': name,
        'description': '', # don't have the correct data for this (if there is any)
        'metadata': False, # note this isn't really correct; we just don't have the relevant data
        'type': 'realtime', # again, not really correct
        # no keywords
        # no groups
      })
      latest_code += 1
  if len(type_docs):
    db[DB_SERVICES_COLLECTION].insert(type_docs)
  

def update_service_codes(collection):
  for service in db[DB_SERVICES_COLLECTION].find():
    db[collection].update({'service_request_type': service['service_name']}, {'$set': {'service_request_code': service['service_code']}})


def combine_requests():
  seen_ids = {}
  for request in db[DB_BASIC_COLLECTION].find():
    del request['_id']
    srn = request['service_request_number']
    if srn in seen_ids:
      db[DB_COMBINED_COLLECTION].update({'_id': seen_ids[srn]}, {'$push': {'records': request}})
    else:
      main = { 'records': [request] }
      for key, value in request.iteritems():
        main[key] = value
      seen_ids[srn] = db[DB_COMBINED_COLLECTION].insert(main)


def clean_up_combined():
  """
  Update the overall status for the combined request (if any of the records are closed, the overall request should be)
  Update the overall created, updated, completed dates
  """
  for request in db[DB_COMBINED_COLLECTION].find():
    # make sure the overall status and created/updated/completed times reflect all the records for each ID
    overall_open = 'open' in request['status']
    min_create = request['created']
    max_complete = 'completed' in request and request['completed'] or None
    max_time = latest(request['updated'], min_create)
    if 'completed' in request:
      max_time = latest(request['completed'], max_time)
    changed = False
    for record in request['records']:
      min_create = earliest(record['created'], min_create)
      if 'completed' in record:
        max_complete = latest(record['completed'], max_complete)
      max_time = latest(max_complete, max_time)
      max_time = latest(record['updated'], max_time)
      if overall_open and 'completed' in record['status']:
        overall_open = False
        request['status'] = record['status']
        changed = True
    if request['created'] != min_create:
      request['created'] = min_create
      changed = True
    if request['updated'] != max_time:
      request['updated'] = max_time
      changed = True
    if max_complete and ('completed' not in request or request['completed'] != max_complete):
      request['completed'] = max_complete
      changed = True
    if changed:
      db[DB_COMBINED_COLLECTION].save(request)


def make_cases():
  """
  Takes the combined requests and compiles cases (sets of requests that led from one to the next) from them.
  Once run, the cases' requests are not necessarily in order and the cases don't have an overall status.
  """
  seen_ids = {}
  for request in db[DB_COMBINED_COLLECTION].find():
    del request['_id']
    try:
      request_id = request['service_request_number']
    except KeyError:
      print request
    follow_id = request['follow_on_service_request']
    request_case = None
    follow_case = None
    if request_id in seen_ids:
      request_case = seen_ids[request_id]  
    if follow_id and follow_id in seen_ids:
      follow_case = seen_ids[follow_id]
      
    if request_case and follow_case:
      new_requests = [request]
      new_requests.extend(db[DB_CASES_COLLECTION].find_one({'_id': follow_case})['requests'])
      db[DB_CASES_COLLECTION].update({'_id': request_case}, {'$pushAll': {'requests': new_requests}})
      db[DB_CASES_COLLECTION].remove({'_id': follow_case})
      for subreq in new_requests:
        seen_ids[subreq['service_request_number']] = request_case
        
    elif request_case:
      db[DB_CASES_COLLECTION].update({'_id': request_case}, {'$push': {'requests': request}})
      
    elif follow_case:
      db[DB_CASES_COLLECTION].update({'_id': follow_case}, {'$push': {'requests': request}})
      seen_ids[request_id] = follow_case
      
    else:
      request_case = db[DB_CASES_COLLECTION].insert({
        'address': request['address'],
        'latitude': request['latitude'],
        'longitude': request['longitude'],
        'location': request['location'],
        'x': request['x_coordinate'],
        'y': request['y_coordinate'],
        'zip': request['zip'],
        'requests': [request],
        'status': None
      })
      seen_ids[request_id] = request_case
      
    if follow_id and not follow_case:
      seen_ids[follow_id] = request_case
      

def refine_cases():
  for case in db[DB_CASES_COLLECTION].find():
    # order the requests
    if len(case['requests']) > 1:
      case['requests'].sort(key=lambda request: request['created'])
    
    first = case['requests'][0]
    last = case['requests'][-1]
    if 'completed' in last['status'] and last['follow_on_service_request'] == None:
      case['status'] = last['status']
    else:
      case['status'] = 'dup' in last['status'] and 'open - dup' or 'open'
    
    case['unending'] = last['follow_on_service_request'] != None
    case['initial_type'] = first['service_request_type']
    case['initial_request'] = first['service_request_number']
    case['created'] = first['created']
    case['updated'] = None
    for request in case['requests']:
      case['updated'] = latest(case['updated'], request['updated'])
    if 'completed' in case['status']:
      case['completed'] = last['completed']
    
    db[DB_CASES_COLLECTION].save(case)


if __name__ == '__main__':
  parser = OptionParser()
  parser.add_option('-f', '--file', dest='file', help='A JSON file from Socrata to parse and import')
  parser.add_option('-s', '--services', action='store_true', dest='services', help='Extract service types from requests')
  parser.add_option('-r', '--combine', action='store_true', dest='combine', help='Combine multiple records for a single service request')
  parser.add_option('--reconcileservices', action='store_true', dest='reconcile_services', help='Update the service codes in the ServiceRequests collection.')
  parser.add_option('--reconcilecombined', action='store_true', dest='reconcile_combined', help='Update the service codes in the ServiceRequestsCombined collection.')
  parser.add_option('-c', '--cases', action='store_true', dest='cases', help='Create "cases," or sets of linked/follow-on service requests')
  parser.add_option('--refinecases', action='store_true', dest='refine_cases', help='Refine the data stored with each case, putting the requests in order, adding initial type, status, etc.')
  parser.add_option('-a', '--all', action='store_true', dest='all', help='Perform all actions; if a file is provided as a first positional argument, load that JSON')
  (options, args) = parser.parse_args()
  
  if options.file or (options.all and len(args) > 0):
    file_to_load = options.file or args[0]
    print 'Loading data from %s' % file_to_load
    load_srs_from_file(file_to_load)
    # and make indexes
    setup_basic_collection()
  
  if options.services or options.all:
    print 'Extracting services from requests'
    setup_services_collection()
    extract_services()
  
  if options.services or options.reconcile_services or options.all:
    print 'Updating service codes in raw services collection'
    update_service_codes(DB_SERVICES_COLLECTION)
  
  # We do this before creating the combined collection so, if it doesn't
  # yet exist, it goes fast and doesn't waste time. combine_requests() will
  # automatically pick up the reconciled codes from the raw services collection.
  if options.services or options.reconcile_combined or options.all:
    print 'Updating service codes in combined services collection'
    update_service_codes(DB_COMBINED_COLLECTION)
  
  if options.combine or options.all:
    print 'De-duplicating service requests'
    combine_requests()
    clean_up_combined()
    setup_combined_collection()
    
  if options.cases or options.all:
    print 'Combining follow-on requests into cases'
    make_cases()
    setup_cases_collection()
  
  if options.refine_cases or options.all:
    print 'Refining case summary data'
    refine_cases()
  