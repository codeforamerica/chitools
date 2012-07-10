import os
import json
import datetime
import traceback
from contextlib import closing
from flask import Flask, render_template, request, abort, make_response
import pymongo
import iso8601
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

app = Flask(__name__)


def connect_db():
    connection = pymongo.Connection(app.config['DB_HOST'], app.config['DB_PORT'])
    connection[app.config['DB_NAME']].authenticate(app.config['DB_USER'], app.config['DB_PASS'])
    # Really shouldn't do this here, but...
    connection[app.config['DB_NAME']][COLLECTION_CASE_INDEX].ensure_index('EID', unique=True, drop_dups=True)
    return connection


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
        if sr and sr['requests'][0]['srs-TYPE_CODE'] in ACCEPTED_SERVICES:
            data = [sr_format.format_case(sr, actual_db)]
            def json_formatter(obj):
                if isinstance(obj, datetime.datetime):
                    return obj.isoformat()
                raise TypeError(repr(o) + " is not JSON serializable")
            
            output = json.dumps(data, default=json_formatter)
            return (output, 200, {'Content-type': 'application/json'})
            
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



if __name__ == "__main__":
    app.config.from_object(__name__)
    # if NIGHTLY_SERVER_SETTINGS in os.environ:
    #     app.config.from_envvar(NIGHTLY_SERVER_SETTINGS)
    
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)