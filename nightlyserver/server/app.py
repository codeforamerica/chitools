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
import sr_format

# Config
DEBUG = True
DB_HOST = 'localhost'
DB_PORT = 27017
DB_USER = 'NightlySRs'
DB_PASS = 'NightlySRs'
DB_NAME = 'NightlySRs'

app = Flask(__name__)


def connect_db():
    connection = pymongo.Connection(app.config['DB_HOST'], app.config['DB_PORT'])
    connection[app.config['DB_NAME']].authenticate(app.config['DB_USER'], app.config['DB_PASS'])
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