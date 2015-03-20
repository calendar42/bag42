```
// REQUIRES JQUERY
// SUGGESTED JQUERY AUTOCOMPLETE

var app = {};

/** Geocode service server address */
app.geocode_serverpath = 'http://bag.plannerstack.com/api/v0/geocode/json';

/** Request a list of possible locations based on the given address. (deferred) */
app.geoCode = function geoCode(address)
{
	var def = $.Deferred();

	if (!address || !(address instanceof String)) {
		console.warn("no valid address given");
		return;
	}

	$.getJSON(app.geocode_serverpath,
	{
		address: address + '*' // The notorious bag42 wildcard
	}).done(function(data)
	{
		if (data.status == 'OK') {
			def.resolve($.map(data.results, function(result)
			{
				var loc = result.geometry.location;
				return {
					address: app.formatAddress(result),
					coord: new app.GeoCoord(loc.lat-0, loc.lng-0)
				};
			}));
		} else {
			def.reject(data.status);
		}
	}).fail(function(jqxhr, textStatus, error)
	{
		def.reject(textStatus);
	});

	return def.promise();
};

app.formatAddress = function (result)
{
	var address = [];

	$.each(result.address_components, function(i, component)
	{
		if (component.types.indexOf('route') >= 0)
			address.unshift(component.long_name);
		else if (component.types.indexOf('street_number') >= 0)
			address.push(component.long_name);
		else if (component.types.indexOf('locality') >= 0)
			address.push(component.long_name);
	});

	return address.length ? address.join(' ') : result.formatted_address;
};

/** Geographic coordinate object: lattitude,longitude */
app.GeoCoord = function GeoCode(lat, long)
{
	if (typeof long == 'string')
	{
		lat = lat.split(',');
		this.lat = lat[0]-0;
		this.long = lat[1]-0;
	}
	else
	{
		this.lat = lat-0;
		this.long = long-0;
	}
};
```
