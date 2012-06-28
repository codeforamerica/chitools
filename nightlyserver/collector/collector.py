import datetime, json
import optparse
from contextlib import contextmanager
import cx_Oracle
import pyproj
import requests
from collector_config import *

SR_FIELDS = (
    "EID",
    "SERVICE_REQUEST_NUM",
    "TYPE_CODE",
    "GROUP_CODE",
    "PRIORITY_CODE",
    "STATUS_CODE",
    "STATUS_DATE",
    "ORIG_SERVICE_REQUEST_EID",
    "CREATION_REASON_CODE",
    "RELATED_REASON_CODE",
    "METHOD_RECEIVED_CODE",
    "VALID_SEGMENT_FLAG",
    "STREET_NUMBER",
    "STREET_NAME_PREFIX",
    "STREET_NAME",
    "STREET_NAME_SUFFIX",
    "STREET_SUFFIX_DIRECTION",
    "CITY",
    "STATE_CODE",
    "COUNTY",
    "ZIP_CODE",
    "UNIT_NUMBER",
    "FLOOR",
    "BUILDING_NAME",
    "LOCATION_DETAILS",
    "X_COORDINATE",
    "Y_COORDINATE",
    "DETAILS",
    "CREATED_DATE",
    "UPDATED_DATE",
    "GEO_AREA_CODE",
    "GEO_AREA_VALUE"
)

ACTIVITY_FIELDS = (
    "EID",
    "SERVICE_REQUEST_EID",
    "ACTIVITY_CODE",
    "DUE_DATE",
    "COMPLETE_DATE",
    "ASSIGNED_STAFF_EID",
    "OUTCOME_CODE",
    "DETAILS",
    "BUSINESS_CODES",
    "CREATED_DATE",
    "CREATED_BY_EID",
    "UPDATED_DATE",
    "UPDATED_BY_EID",
    "PRECEDED_BY_EID",
    "COMPLETED_DATE_TIMESTAMP",
)

GROUP_CODE_FIELDS = (
    "DESCRIPTION",
)

ACTIVITY_CODE_FIELDS = (
    "DESCRIPTION",
)

# correctly named fields for querying related to activities
ACTIVITY_FIELD_NAMES = \
    map(lambda x: 'act.' + x, ACTIVITY_FIELDS) + \
    map(lambda x: 'codes_act.' + x, ACTIVITY_CODE_FIELDS)

# correctly named fields for querying related to SRs
SR_FIELD_NAMES = \
    map(lambda x: 'srs.' + x, SR_FIELDS) + \
    map(lambda x: 'codes_group.' + x, GROUP_CODE_FIELDS)

# correctly named fields for the full query
FIELD_NAMES = SR_FIELD_NAMES + ACTIVITY_FIELD_NAMES


projector = None
if PROJECTION:
    projector = pyproj.Proj(PROJECTION, preserve_units=True)

dsn = cx_Oracle.makedsn(DB_PATH, DB_PORT, DB_NAME)
db = cx_Oracle.connect(DB_USER, DB_PASS, dsn)

cur = db.cursor()

def get_for_dates(start_date, end_date=None):
    # clean up dates
    start_day = start_date.strftime('%d-%b-%y')
    if not end_date:
        end_date = start_date + datetime.timedelta(1)
    end_day = end_date.strftime('%d-%b-%y')
    
    # do the query
    cur.execute("""SELECT %s 
        FROM SERVICE_REQUESTS srs 
            LEFT JOIN SR_ACTIVITIES act 
                ON srs.EID = act.SERVICE_REQUEST_EID
            LEFT JOIN CODE_DESCRIPTIONS codes_group
                ON srs.GROUP_CODE = codes_group.CODE_CODE AND codes_group.TYPE_CODE = 'GROUP'
            LEFT JOIN CODE_DESCRIPTIONS codes_act
                ON act.ACTIVITY_CODE = codes_act.CODE_CODE AND codes_act.TYPE_CODE = 'SRACTVTY'
        WHERE srs.CREATED_DATE >= '%s' AND srs.CREATED_DATE < '%s'""" % (', '.join(FIELD_NAMES), start_day, end_day))
    
    return cur.fetchall()


def clean_results(results):
    srs = {}
    for row in results:
        sr = None
        if row[1] in srs:
            sr = srs[row[1]]
        else:
            sr = {'activities': []}
            srs[row[1]] = sr
            for index, field_name in enumerate(SR_FIELD_NAMES):
                sr[field_name] = row[index]
            # for index, value in enumerate(row):
            #     sr[FIELD_NAMES[index]] = value
            if projector:
                x = sr['srs.X_COORDINATE']
                y = sr['srs.Y_COORDINATE']
                if x and y:
                    longitude, latitude = projector(x, y, inverse=True)
                    sr['srs.X_COORDINATE'] = longitude
                    sr['srs.Y_COORDINATE'] = latitude
        
        # if there are no activities, act.EID (the first activity field) will be None
        activity_index = len(SR_FIELD_NAMES)
        if row[activity_index]:
            activity = {}
            for index, field_name in enumerate(ACTIVITY_FIELD_NAMES):
                activity[field_name] = row[activity_index + index]
        
            sr['activities'].append(activity)
    
    return srs


def sr_json_encoder(obj):
    # Dates will have "date::" in front so they are easy to identify
    if isinstance(obj, datetime.datetime):
        return 'date::%s' % obj.isoformat()
    raise TypeError(repr(o) + " is not JSON serializable")


@contextmanager
def debug_timer(message=''):
    start = datetime.datetime.now()
    yield start
    elapsed = datetime.datetime.now() - start
    print '%s: %ss' % (message, elapsed.seconds + (elapsed.microseconds / 1000000.0))



############### API YOU WANT TO USE #################
def do_date_range(start, end=None, save=False, send=True):
    if not end:
        end = start + datetime.timedelta(1)
        
    the_date = start
    while the_date < end:
        do_date(the_date, save, send)
        the_date = the_date + datetime.timedelta(1)

def do_date(the_date, save=False, send=True):
    print '%s:' % the_date.strftime('%y-%m-%d')
    
    with debug_timer('  Overall'):
        with debug_timer('  Get data'):
            results = get_for_dates(the_date)
        with debug_timer('  Parse and clean'):
            data = clean_results(results)
        with debug_timer('  encode'):
            encoded = json.dumps(data.values(), default=sr_json_encoder)
            
    # Save to file
    if save:
        f = open('nightlydata_%s.json' % the_date.strftime('%y-%m-%d'), 'w')
        f.write(encoded)
        f.close()
        
    if send:
        send_url = isinstance(send, basestring) and send or DEFAULT_SEND_URL
        with debug_timer('  Post to server'):
            requests.post(send_url, data=encoded, headers={'content-type': 'application/json'})



if __name__ == '__main__':
    # parser = OptionParser()
    # parser.add_option("-u", "--url", dest="url", help="URL to send to")
    
    start_day = datetime.date.today() - datetime.timedelta(4)
    do_date_range(start_day, datetime.date.today())
    
    