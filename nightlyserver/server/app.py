import os
import json
import datetime
import traceback
from contextlib import closing
from flask import Flask, render_template, request, abort
import pymongo
import iso8601
from handle_srs import *
from accepted_services import *
from db_info import *

# Config
DEBUG = True
DB_HOST = 'localhost'
DB_PORT = 27017
DB_USER = ''
DB_PASS = ''
DB_NAME = 'NightlySRs'

app = Flask(__name__)

def connect_db():
    connection = pymongo.Connection(app.config['DB_HOST'], app.config['DB_PORT'])
    # Really shouldn't do this here, but...
    connection[app.config['DB_NAME']][COLLECTION_CASE_INDEX].ensure_index('EID', unique=True, drop_dups=True)
    return connection

@app.route("/")
def index():
    return "Chicago Nightly 311";


@app.route("/api/services.json")
def api_services():
    return ""


@app.route("/api/requests/<request_id>.json")
def api_get_request(request_id):
    with closing(connect_db()) as db:
        actual_db = db[DB_NAME]
        sr = actual_db[COLLECTION_CASES].find_one({"_id": request_id})
        if sr:# and sr['requests'][0]['srs-TYPE_CODE'] in ACCEPTED_SERVICES:
            # Lots of fixing up needed here, like merging requests and their activities in to a flat list
            case_items = sorted(sr['requests'], key=lambda item: item['srs-CREATED_DATE'])
            activities = []
            for index, subrequest in enumerate(case_items):
                if index > 0:
                    # create an activity for follow-ons
                    activity = {
                        'datetime': subrequest['srs-CREATED_DATE'],
                        'description': get_service_by_code(subrequest['srs-TYPE_CODE']) or subrequest['srs-TYPE_CODE'], #subrequest['srs-TYPE_CODE'], # FIXME: this should be the type *name*
                        'type': 'subrequest',
                        'properties': {
                            'service_request_id': subrequest['srs-SERVICE_REQUEST_NUM'],
                            'service_code': subrequest['srs-TYPE_CODE'],
                            'agency_responsible': subrequest['codes_group-DESCRIPTION'],
                            # TODO: should this carry more info?
                            'details': subrequest['srs-DETAILS'],
                        }
                    }
                    activities.append(activity)
                    
                for sr_activity in subrequest['activities']:
                    if sr_activity['act-COMPLETE_DATE']:
                        activity = {
                            'datetime': sr_activity['act-COMPLETE_DATE'],
                            'description': sr_activity['codes_act-DESCRIPTION'],
                            # TODO: there is probably more to add here
                            'type': 'activity',
                            'properties': {}
                        }
                        if 'act-DETAILS' in sr_activity and sr_activity['act-DETAILS']:
                            activity['properties']['details'] = sr_activity['act-DETAILS']
                        activities.append(activity)
            
            last_sr = case_items[-1]
            overall_status = last_sr['srs-STATUS_CODE'].startswith('O-') and 'open' or 'closed'
            if overall_status == 'closed':
                activities.append({
                    'datetime': last_sr['srs-UPDATED_DATE'],
                    'description': 'Service request completed.',
                    'type': 'closed'
                })
                
            activities.sort(key=lambda activity: activity['datetime'])
            def json_formatter(obj):
                if isinstance(obj, datetime.datetime):
                    return obj.isoformat()
                raise TypeError(repr(o) + " is not JSON serializable")
            
            sr['requests'][0]['service_name'] = get_service_by_code(sr['requests'][0]['srs-TYPE_CODE'])
            
            status_notes = len(activities) and activities[-1]['description'] or None
            
            base_sr = sr['requests'][0]
            
            address_string = str(base_sr['srs-STREET_NUMBER'])
            if base_sr['srs-STREET_NAME_PREFIX']:
                address_string = address_string + ' ' + base_sr['srs-STREET_NAME_PREFIX']
            address_string = address_string + ' ' + base_sr['srs-STREET_NAME']
            if base_sr['srs-STREET_SUFFIX_DIRECTION']:
                address_string = address_string + ' ' + base_sr['srs-STREET_SUFFIX_DIRECTION']
            if base_sr['srs-STREET_NAME_SUFFIX']:
                address_string = address_string + ' ' + base_sr['srs-STREET_NAME_SUFFIX']
            address_string = '%s, %s, %s' % (address_string, base_sr['srs-CITY'], base_sr['srs-STATE_CODE'])
            
            
            body = render_template(
                'request.json', 
                sr=sr['requests'][0], 
                activities=json.dumps(activities, default=json_formatter), 
                status=overall_status,
                status_notes=status_notes,
                address=address_string)
            
            return (body, 200, {'Content-type': 'application/json'})
            
    return ("No such service request", 404, None)


@app.route("/receive", methods=['POST'])
def receive():
    data = request.json
    if not data:
        print 'No or bad JSON.'
        return ("You must POST a JSON array to this URL.", 400, None)
    
    with closing(connect_db()) as db:
        actual_db = db[DB_NAME]
        for sr in data:
            try:
                save_sr_data(sr, actual_db)
            except Exception, e:
                # print '!! Receive error: %s' % e.message
                traceback.print_exc()
    
    return ""


@app.route("/receive_types", methods=['POST'])
def receive_types():
    data = request.json
    if not data:
        print 'No or bad JSON.'
        return ("You must POST a JSON object or array to this URL.", 400, None)

    with closing(connect_db()) as db:
        actual_db = db[DB_NAME]
        try:
            save_sr_type_data(data, actual_db)
        except Exception, e:
            traceback.print_exc()

    return ""


def get_service_by_code(code, db=None):
    """Get the service name associated with a service code."""
    # FIXME: this should probably cache the list of services instead of hitting the DB
    if not db:
        db = connect_db()
        result = get_service_by_code(code, db)
        db.close()
        return result
    
    service = db[DB_NAME][COLLECTION_SERVICES].find_one({'_id': code})
    return service and service['name'] or None
        



if __name__ == "__main__":
    app.config.from_object(__name__)
    # if NIGHTLY_SERVER_SETTINGS in os.environ:
    #     app.config.from_envvar(NIGHTLY_SERVER_SETTINGS)
    
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)