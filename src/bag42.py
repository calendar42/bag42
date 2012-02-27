import MySQLdb
import math
import uwsgi
import simplejson
from webob import Request


# http://www.onzetaal.nl/taaladvies/advies/afkortingen-van-provincienamen
provincies_nl_kort = {
'Drenthe': 'DR',
'Flevoland': 'FL',
'Friesland': 'FR',
'Gelderland': 'GD',
'Groningen': 'GR',
'Limburg': 'LB',
'Noord-Brabant': 'NB',
'Noord-Holland': 'NH',
'Overijssel': 'OV',
'Utrecht': 'UT',
'Zuid-Holland': 'ZH',
'Zeeland': 'ZL',
}

provincies_be_kort = {
'Antwerpen': 'ANT',
'Henegouwen': 'HAI',
'Luik': 'LIE',
'Limburg': 'LIM',
'Luxemburg': 'LUX',
'Namen': 'NAM',
'Oost-Vlaanderen': 'OVL',
'Vlaams-Brabant': 'VBR',
'Waals-Brabant': 'WBR',
'West-Vlaanderen': 'WVL',
}

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
			if x[0] in tmp2:
	                        tmp3.append(tmp2[x[0]])

                return tmp3

def google_json(straat, huisnummer, huisletter, huisnummertoevoeging, postcode, woonplaats, gemeente, provincie, buurt, wijk, lat, lon):
	if straat != '':
		return {'types': [ "streetaddress" ],
			'formatted_address': "%s %s%s%s\n%s  %s" % (straat, huisnummer, huisletter, huisnummertoevoeging, postcode, woonplaats),
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
				'long_name': woonplaats,
				'short_name': woonplaats,
				'types': [ "locality" ],
			},
			{
				'long_name': gemeente,
				'short_name': gemeente,
				'types': [ "administrative_area_level_2", "political" ],
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
				'short_name': provincies_nl_kort[provincie],
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

	elif woonplaats != '':
		return {'types': [ "locality" ],
			'address_components': [
			{
				'long_name': woonplaats,
				'short_name': woonplaats,
				'types': [ "locality" ],
			},
			{
				'long_name': gemeente,
				'short_name': gemeente,
				'types': [ "administrative_area_level_2", "political" ],
			},
			{
				'long_name': provincie,
				'short_name': provincies_nl_kort[provincie],
				'types': [ "administrative_area_level_1", "political" ],
			},
			{
				'long_name': 'Nederland',
				'short_name': 'NL',
				'types': [ "country", "political" ],
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

	elif provincie != '':
		return {'types': [ "administrative_area_level_1" ],
			'address_components': [
			{
				'long_name': provincie,
				'short_name': provincies_nl_kort[provincie],
				'types': [ "administrative_area_level_1", "political" ],
			},
			{
				'long_name': 'Nederland',
				'short_name': 'NL',
				'types': [ "country", "political" ],
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

	else:
		return None


def google_reply(rows):
	if rows is None:
		yield simplejson.dumps({'status': "INVALID_REQUEST"})

	elif rows == 0:
		yield simplejson.dumps({'status': "ZERO_RESULTS"})

	else:
		reply = {'status': 'OK'}
		results = []
		for row in rows:
			results.append(google_json(row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], math.degrees(row[12]), math.degrees(row[11])))

		yield simplejson.dumps({'status': "OK", 'results': results})

	return

def tileindex(lat, lon):
	difflat = round(lat * 1000)
	difflon = round(lon * 1000)

	tileslat = [int(difflat)]
	tileslon = [int(difflon)]

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
		tileslon.append(tileslon[0] - 1)

	return '"'+' '.join(["%dx%d" % (x,y) for x in tileslon for y in tileslat])+'"/1'


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

			print lat, lon, geoindex

			db = MySQLdb.connect(host="127.0.0.1", port=9306)
			c = db.cursor()
			c.execute("""SELECT id, geodist(%s, %s, lat_radians, lon_radians) AS distance FROM bag WHERE match(%s) ORDER BY distance ASC LIMIT 100""", (lat, lon, geoindex))
			rows = c.fetchall()
			if rows is not None:
				rows_final = [rows[0]]
				i = 1
				for row in rows[1:]:
					if rows[0][1] == row[1]:
						rows_final.append(row)
					else:
						break

				rows = fetchall(c, rows_final)

			c.close()

			return google_reply(rows)

		except:
			raise
			return google_reply(None)

	elif 'address' in request.params:
		try:
			maxitems = int(request.params['maxitems'])
		except:
			maxitems = 10
				
		address = request.params["address"].replace('+', ' ')
		db = MySQLdb.connect(host="127.0.0.1", port=9306)
		c = db.cursor()
		c.execute("""SELECT * FROM bag, bag_metaphone, bag_woonplaats, bag_provincie, tudelft WHERE match(%s) LIMIT %s OPTION index_weights=(bag=1000, bag_metaphone=1, bag_woonplaats=1850, bag_provincie=1600, tudelft=10), ranker=sph04, field_weights=(straat=5,huisnummer=3,huisletter=2,huisnummertoevoeging=1,postcode=15,woonplaats=15,gemeente=4,provincie=4,buurt=3,wijk=3);""", (address,maxitems))
		rows = fetchall(c, c.fetchall())
		c.close()

		return google_reply(rows)

	else:
		return google_reply(None)

uwsgi.applications = {'':bag42}
