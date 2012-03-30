"""
Tools for exploring the tree of possible follow-ons for any given request type or set of request types.
"""

def check_deepness(db, types, seen_types=None):
  """
  Recursively identify request types that result as follow-ons from a given set of initial request types.
  db = The name of the database to use
  types = A list of request type names (not codes) to start from
  """
  overall_count = db.ServiceRequestsCombined.find({'service_request_type': {'$in': types}}).count()
  
  follows = []
  followed_types = {}
  followed_count = 0
  for request in db.ServiceRequestsCombined.find({'service_request_type': {'$in': types}}):
    if request['follow_on_service_request']:
      follows.append(request['follow_on_service_request'])
      followed_types[request['service_request_type']] = True
      followed_count += 1
  
  # if followed_count == 0:
  #   return None
  
  following_types = {}
  following_count = 0
  for request in db.ServiceRequestsCombined.find({'service_request_number': {'$in': follows}}):
    following_types[request['service_request_type']] = True
    following_count += 1
  
  deeper_types = following_types.keys()
  seen_types = seen_types or []
  seen_types.extend(types)
  for key in following_types.keys():
    if key in seen_types:
      deeper_types.remove(key)
  
  return {
    'record_count': overall_count,
    'followed_types': followed_types.keys(),
    'following_types': following_types.keys(),
    'followed_count': followed_count,
    'following_count': following_count,
    'next': len(deeper_types) and check_deepness(db, deeper_types, seen_types) or None,
  }


def print_deepness(db, types, title=None, single_line=False):
  """
  Prettily prints the results of check_deepness.
  db = The name of the database to use
  types = A list of request type names (not codes) to start from
  title = A title to print before the results
  single_line = If true, print each follow-on type on its own line (instead of a comma-separated list)
  """
  result = check_deepness(db, types)
  if title:
    print '%s\n%s'% (title, len(title) * '=')
  else:
    print '===================='
  
  depth = 1
  data = result
  print 'Types: %s (%s)' % (', '.join(data['followed_types']), data['record_count'])
  while data:
    space = depth * '  '
    text = 'Followed by: '
    if data['followed_count'] == 0:
      print '%sNO FOLLOW-ONS!' % space
    else:
      line_start = '%s%s Followed by: ' % (space, data['followed_count'])
      follow_join = single_line and ('\n%s' % (len(line_start) * ' ')) or ', '
      print '%s%s' % (line_start, follow_join.join(data['following_types']) or '????')
    data = data['next']
    depth += 1
    
  print '\n\n'



types_animal = type_names = ["Vicious Animal",
"Stray Animal",
"Animal Bite",
"Animal Abandoned",
"Unwanted Animal",
"Nuisance Animals",
"Animal - Inhumane Treatment",
"Injured Animal",
"Animal Fighting",
"Dead Animal Pick-Up",
"Animal Business",
"Animal In Trap",
"Dangerous Dog",]

types_rodent = ["Park Rodent Abatement","Rodent Baiting/Rat Complaint",]

types_trees = ["Tree Debris","Tree Emergency","Tree Trim","Tree Planting","Tree Planting - Zoning","Tree Removal","Stump Removal","Tree Planting - Green Streets",]

types_graffiti = ["Graffiti Removal",]

types_sewer = ["Clean Main Sewer (DWM Use Only)","Sewer Cave In Inspection","Sewer Odor/Bad Odor","Sewer Cleaning Inspection","Alley Sewer Inspection","Repair Main Sewer (DWM Use Only)","Clean Alley Sewer (DWM Use Only)",]

types_dumping = ["Fly Dumping","Fly Dump (Tires)",]

types_street_lights = ["Street Light Pole Door Missing","Viaduct Lights Out","Alley Light New","Alley Light Out","Street Lights - All/Out","Street Light - 1/Out","Street Light Pole Damage","Street Lights On Days",]

types_traffic_lights = ["Traffic Light Out","Red Light Camera","Traffic Light Study","Traffic Light Timing",]

types_building = ["Building Violation","No Building Permit & Construction Violations","Building - Illegal Conversion",]

if __name__ == '__main__':
  db = '311Data'
  print_deepness(db, types_animal, 'ANIMALS')
  print_deepness(db, types_rodent, 'RODENT')
  print_deepness(db, types_trees, 'TREES')
  print_deepness(db, types_graffiti, 'GRAFFITI')
  print_deepness(db, types_dumping, 'DUMPING')
  print_deepness(db, types_street_lights, 'STREET LIGHTS')
  print_deepness(db, types_traffic_lights, 'TRAFFIC LIGHTS')
  print_deepness(db, types_sewer, 'SEWER', True)
  print_deepness(db, types_building, 'BUILDING', True)
