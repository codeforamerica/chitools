import os
import json
import traceback
from contextlib import closing
from flask import Flask, render_template, request, abort
import pymongo
import iso8601
from handle_srs import *
from accepted_services import *

# Config
DEBUG = True
DB_HOST = 'localhost'
DB_PORT = 27017
DB_USER = ''
DB_PASS = ''
DB_NAME = 'NightlySRs'

COLLECTION_CASE_INDEX = 'CaseIndex'
COLLECTION_CASES      = 'Cases'
COLLECTION_ORPHANS    = 'OrphanCases'


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
        if sr and sr['requests'][0]['srs-TYPE_CODE'] in ACCEPTED_SERVICES:
            # Lots of fixing up needed here, like merging requests and their activities in to a flat list
            return render_template('request.json', sr=sr['requests'][0])
            
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





if __name__ == "__main__":
    app.config.from_object(__name__)
    # if CLOSED_STREETS_SETTINGS in os.environ:
    #     app.config.from_envvar(CLOSED_STREETS_SETTINGS)
    
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)