import MySQLdb
import math
import uwsgi
import simplejson
from webob import Request

def fetchall(c, result):
        if len(result) == 0:
                return None
        elif len(result) > 0:
                ids = ','.join([str(x[0]) for x in result])
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

def google_json(straat, huisnummer, huisletter, huisnummertoevoeging, postcode, gemeente, provincie, buurt, wijk, lat, lon):
	return {'types': [ "streetaddress" ],
		'formatted_address': "%s %s%s%s\n%s  %s" % (straat, huisnummer, huisletter, huisnummertoevoeging, postcode, gemeente),
		'address_components': [
		{
			'long_name': huisnummer,
			'short_name': huisnummer,
			'types': [ "street_number" ],
		},
		{
			'long_name': straat,
			'short_name': straat,
			'types': [ "route" ],
		},
		{
			'long_name': gemeente,
			'short_name': gemeente,
			'types': [ "locality", "political" ],
		},
		{
			'long_name': provincie,
			'short_name': provincie,
			'types': [ "administrative_area_level_1", "political" ],
		},
		{
			'long_name': 'Nederland',
			'short_name': 'NL',
			'types': [ "country", "political" ],
		},
		{
			'long_name': postcode,
			'short_name': postcode,
			'types': [ "postcode_code" ],
		},
		],
		'geometry': { 'location': { 'lat': lat, 'lng': lon }, 'location_type': 'GEOMETRIC_CENTER' }
		}

def google_reply(rows):
	if rows is None:
		yield simplejson.dumps({'status': "INVALID_REQUEST"})

	elif len(rows) == 0:
		yield simplejson.dumps({'status': "ZERO_RESULTS"})

	else:
		reply = {'status': 'OK'}
		results = []
		for row in rows:
			results.append(google_json(row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], math.degrees(row[11]), math.degrees(row[10])))

		yield simplejson.dumps({'status': "OK", 'results': results})

	return	

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

			lat = math.radians(lat)
			lon = math.radians(lon)

			tileindex = "%dx%d" % (int(lon * 1000), int(lat * 1000))

			db = MySQLdb.connect(host="127.0.0.1", port=9306)
			c = db.cursor()
			c.execute("""SELECT id, geodist(%s, %s, lat_radians, lon_radians) AS distance FROM bag WHERE match(%s) ORDER BY distance ASC LIMIT 1;""", (lat, lon, tileindex))
			rows = fetchall(c, c.fetchall())
			c.close()

			return google_reply(rows)

		except:
			return googlereply(None)

	elif 'address' in request.params:
		address = request.params["address"]
		db = MySQLdb.connect(host="127.0.0.1", port=9306)
		c = db.cursor()
		c.execute("""SELECT * FROM bag, bag_metaphone WHERE match(%s) LIMIT 10 OPTION index_weights=(bag=100, bag_metaphone=10);""", (address,))
		rows = fetchall(c, c.fetchall())
		c.close()

		return google_reply(rows)

	else:
		return google_reply(None)

uwsgi.applications = {'':bag42}
