import MySQLdb
import math
import uwsgi
import simplejson
from webob import Request

def fetchall(c, result):
        if len(result) == 0:
                return None
        elif len(result) > 0 and len(result[0]) == 1:
                ids = ','.join([str(x[0]) for x in result])
                #print ids
                # The following line will not allow injection, it consist only of ids
                c.execute("""select * from bag where id IN (%s)"""%(ids))
                tmp = c.fetchall()
                tmp2 = {}
                tmp3 = []
                for x in tmp:
                        tmp2[x[0]] = x

                for x in result:
                        tmp3.append(tmp2[x[0]])

                return tmp3
        else:
                return result


def bag42(environ, start_response):
	start_response('200 OK', [('Content-Type', 'application/json'), ('Access-Control-Allow-Origin', '*')])
	request = Request(environ)

	lat = None
	lon = None

	if 'latlng' in request.params:
		try:
			lat, lon = request.params["latlng"].split(',')
			lat = float(lat)
			lon = float(lon)
		except:
			yield simplejson.dumps({'status': "INVALID_REQUEST"})
			return

	elif 'address' in request.params:
		address = request.params["address"]
		db = MySQLdb.connect(host="127.0.0.1", port=9306)
		c = db.cursor()
		c.execute("""select * from bag, bag_metaphone where match(%s) limit 10 option index_weights=(bag=100, bag_metaphone=10);""", (address,))
		rows = fetchall(c, c.fetchall())
		c.close()

		if len(rows) == 0:
			yield simplejson.dumps({'status': "ZERO_RESULTS"})
			return
		else:
			reply = {'status': 'OK'}
			results = []
			for row in rows:
				results.append({'types': [ "streetaddress" ],
				                'formatted_address': "%s %s%s%s\n%s  %s" % (row[1], row[2], row[3], row[4], row[5], row[6]),
						'address_components': [
						{
							'long_name': row[2],
							'short_name': row[2],
							'types': [ "street_number" ],
						},
						{
							'long_name': row[1],
							'short_name': row[1],
							'types': [ "route" ],
						},
						{
							'long_name': row[6],
							'short_name': row[6],
							'types': [ "locality", "political" ],
						},
						{
							'long_name': row[7],
							'short_name': row[7],
							'types': [ "administrative_area_level_1", "political" ],
						},
						{
							'long_name': 'Nederland',
							'short_name': 'NL',
							'types': [ "country", "political" ],
						},
						{
							'long_name': row[5],
							'short_name': row[5],
							'types': [ "postcode_code" ],
						},
						],
						'geometry': { 'location': { 'lat': math.degrees(row[11]), 'lon': math.degrees(row[10]) }, 'location_type': 'GEOMETRIC_CENTER' }
						})

			yield simplejson.dumps({'status': "OK", 'results': results})
			return	

	else:
		yield simplejson.dumps({'status': "INVALID_REQUEST"})
		return

uwsgi.applications = {'':bag42}
