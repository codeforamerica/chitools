import os
import json
import datetime
import traceback
from contextlib import closing
import logging
from flask import Flask, render_template, request, abort, make_response
import pymongo
from dateutil.parser import parse as parse_date
from handle_srs import *
from accepted_services import *
from db_info import *
import sr_format

# Config
# NOTE: in production, you should pull in different config information.
# The easiest way to do this is to call:
#     application.config.from_object('<config_file>')
# from your WSGI file.
DEBUG = True
DB_HOST = 'localhost'
DB_PORT = 27017
DB_USER = 'NightlySRs'
DB_PASS = 'NightlySRs'
DB_NAME = 'NightlySRs'
REQUIRE_KEY = False
MAX_PAGE_SIZE = 250
DEFAULT_PAGE_SIZE = 50

app = Flask(__name__)

# Logging
LOG_LEVEL = logging.ERROR
LOG_PATH = os.environ.get('NIGHTLY_SR_LOG_PATH', None)
logging.basicConfig(level=LOG_LEVEL, filename=LOG_PATH)


def connect_db():
    connection = pymongo.Connection(app.config['DB_HOST'], app.config['DB_PORT'])
    connection[app.config['DB_NAME']].authenticate(app.config['DB_USER'], app.config['DB_PASS'])
    # Really shouldn't do this here, but...
    connection[app.config['DB_NAME']][COLLECTION_CASE_INDEX].ensure_index('EID', unique=True, drop_dups=True)
    return connection


def flattened_arg_list(arg_name):
    flattened = None
    if arg_name in request.args:
        flattened = []
        for item in request.args.getlist(arg_name):
            flattened.extend(map(lambda subitem: subitem.strip(), item.split(',')))
    return flattened


@app.before_request
def always_require_api_key(*args, **kwargs):
    if app.config['REQUIRE_KEY']:
        # was an API included?
        if 'api_key' not in request.args:
            return make_response(
                json.dumps({'error': 'You must include an API Key ("?api_key=...") for ALL requests to this endpoint.'}),
                400,
                {'Content-type': 'application/json'})

        # Check the key's validity
        with connect_db() as db:
            key = request.args['api_key']
            key_info = db[app.config['DB_NAME']][COLLECTION_API_KEYS].find_one({'_id': key})
            if not key_info:
                return make_response(
                    json.dumps({'error': 'Invalid API Key.'}),
                    401,
                    {'Content-type': 'application/json'})


@app.after_request
def make_jsonp(response):
    extension = request.path.rpartition('.')[2]
    callback = request.args.get('callback')
    if extension in ('json', 'jsonp') and callback:
        response.data = '%s(%s)' % (callback, response.data)
    return response


@app.route("/")
def index():
    return "Chicago Nightly 311";


@app.route("/api/services.json")
def api_services():
    with closing(connect_db()) as db:
        actual_db = db[DB_NAME]
        rows = actual_db.Services.find({"_id": {"$in": ACCEPTED_SERVICES}})
        
    services = []
    for row in rows:
        services.append({
            # Use a custom UUID to match Spot Reporter
            'service_code': row['uuid'],
            'service_name': row['name'],
            'description': row.get('description'),
            'metadata': False,
            'type': 'batch',
            'group': row.get('department'),
            # Include internal service code as keyword
            'keywords': 'code:%s' % row['_id'],
        })
    return make_response(
        json.dumps(services),
        200,
        {'Content-type': 'application/json'})


@app.route("/api/requests/<request_id>.json")
def api_get_request(request_id):
    with closing(connect_db()) as db:
        actual_db = db[DB_NAME]
        sr = actual_db[COLLECTION_CASES].find_one({"_id": request_id})
        if sr and sr['requests'][0]['srs-TYPE_CODE'] in ACCEPTED_SERVICES:
            data = [sr_format.format_case(sr, actual_db)]
            def json_formatter(obj):
                if isinstance(obj, datetime.datetime):
                    return obj.isoformat()
                raise TypeError(repr(o) + " is not JSON serializable")
            
            output = json.dumps(data, default=json_formatter)
            return (output, 200, {'Content-type': 'application/json'})
            
    return ("No such service request", 404, None)


@app.route("/api/requests.json")
def api_get_requests():
    # paging
    page_size = app.config['DEFAULT_PAGE_SIZE']
    if 'page_size' in request.args and request.args['page_size'].isdigit():
        page_size = int(request.args['page_size'])  
    page_size = min(app.config['MAX_PAGE_SIZE'], page_size)
    if page_size <= 0:
        page_size = app.config['DEFAULT_PAGE_SIZE']
    page = 1
    if 'page' in request.args and request.args['page'].isdigit():
        page = max(1, int(request.args['page']))
    
    # date ranges
    start_requested_datetime = request.args.get('start_date', type=parse_date)
    end_requested_datetime = request.args.get('end_date', type=parse_date)
    start_updated_datetime = request.args.get('start_updated_date', type=parse_date)
    end_updated_datetime = request.args.get('end_updated_date', type=parse_date)
    # NOTE: no attempt to limit the dates to 90 day ranges as per spec because we are doing paging
    
    # listed args can come in two formats:
    # ?service_request_id=id&service_request_id=id
    # ?service_request_id=id,id
    service_request_id = flattened_arg_list('service_request_id')
    status = flattened_arg_list('status')
    service_code = flattened_arg_list('service_code')
    # limit service codes to the accepted ones
    if service_code:
        service_code = filter(lambda code: code in ACCEPTED_SERVICES, service_code)
    else:
        service_code = ACCEPTED_SERVICES
    
    
    # CUSTOM: datetime_type (one of 'requested' or 'updated')
    order_default = (not start_requested_datetime and not end_requested_datetime and (start_updated_datetime or end_updated_datetime)) and 'updated' or 'requested'
    order_by = request.args.get('order_by', default=order_default, type=lambda value: value in ('requested', 'updated') and value or order_default)
    order_by = '%s_datetime' % order_by
    
    with closing(connect_db()) as db:
        actual_db = db[DB_NAME]
        query = {}
        if start_requested_datetime or end_requested_datetime:
            date_query = {}
            if start_requested_datetime:
                date_query['$gte'] = start_requested_datetime
            if end_requested_datetime:
                date_query['$lte'] = end_requested_datetime
            query['requested_datetime'] = date_query
        if start_updated_datetime or end_updated_datetime:
            date_query = {}
            if start_updated_datetime:
                date_query['$gte'] = start_updated_datetime
            if end_updated_datetime:
                date_query['$lte'] = end_updated_datetime
            query['updated_datetime'] = date_query
        if service_request_id:
            query['_id'] = {'$in': service_request_id}
        if service_code:
            query['service_code'] = {'$in': service_code}
        if status:
            query['status'] = {'$in': status}
            
        srs = actual_db[COLLECTION_CASES].find(query).sort(order_by, pymongo.DESCENDING).skip((page - 1) * page_size).limit(page_size)
        data = map(lambda sr: sr_format.format_case(sr, actual_db), srs)
        def json_formatter(obj):
            if isinstance(obj, datetime.datetime):
                return obj.isoformat()
            raise TypeError(repr(o) + " is not JSON serializable")
        
        output = json.dumps(data, default=json_formatter)
        return (output, 200, {'Content-type': 'application/json'})


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



if __name__ == "__main__":
    app.config.from_object(__name__)
    # if NIGHTLY_SERVER_SETTINGS in os.environ:
    #     app.config.from_envvar(NIGHTLY_SERVER_SETTINGS)
    
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)