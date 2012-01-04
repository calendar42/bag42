import MySQLdb
import math
import uwsgi
import simplejson
from webob import Request

def fetchall(c, result):
	if result is None:
		return result
        elif len(result) == 0:
                return 0
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
			'long_name': wijk,
			'short_name': wijk,
			'types': [ "sublocality" ],
		},
		{
			'long_name': buurt,
			'short_name': buurt,
			'types': [ "neighborhood" ],
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
		'geometry':
		{
			'location': { 'lat': "%.8f" % lat, 'lng': "%.8f" % lon },
			'location_type': 'GEOMETRIC_CENTER',
			'viewport':
			{
				'southwest': { 'lat': "%.6f" % (lat - 0.003), 'lng': "%.6f" % (lon - 0.003) },
				'northeast': { 'lat': "%.6f" % (lat + 0.003), 'lng': "%.6f" % (lon + 0.003) },
			},
		}
		}

def google_reply(rows):
	if rows is None:
		yield simplejson.dumps({'status': "INVALID_REQUEST"})

	elif rows == 0:
		yield simplejson.dumps({'status': "ZERO_RESULTS"})

	else:
		reply = {'status': 'OK'}
		results = []
		for row in rows:
			results.append(google_json(row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], math.degrees(row[11]), math.degrees(row[10])))

		yield simplejson.dumps({'status': "OK", 'results': results})

	return

def tileindex(lat, lon):
	difflat = lat * 1000
	difflon = lon * 1000

	tileslat = [int(difflat)]
	tileslon = [int(diflon)]

	difflat -= tileslat[0]
	difflon -= tileslon[0]

	difflat = int(difflat) * 10
	difflon = int(difflon) * 10

	if difflat > 5:
		tileslat.append(tileslat[0] + 1)
	elif difflat < 5:
		tileslat.append(tileslat[0] - 1)

	if difflon > 5:
		tileslon.append(tileslon[0] + 1)
	elif difflon < 5:
		tileslat.append(tileslon[0] - 1)

	return ' '.join(["%dx%d" % (x, y) for x in tileslon for y in tileslat])


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

			geoindex = tileindex(lat, lon)

			db = MySQLdb.connect(host="127.0.0.1", port=9306)
			c = db.cursor()
			c.execute("""SELECT id, geodist(%s, %s, lat_radians, lon_radians) AS distance FROM bag WHERE match(%s) ORDER BY distance ASC LIMIT 1;""", (lat, lon, geoindex))
			rows = fetchall(c, c.fetchall())
			c.close()

			return google_reply(rows)

		except:
			return google_reply(None)

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
