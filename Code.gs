function getAuthType() {
  var response = { type: 'NONE' };
  return response;
}

function isAdminUser() {
  return true;
}

// as in received data
var PM10 = "PM10_AirPollutantValue";
var PM25 = "PM2.5_AirPollutantValue";

function getConfig(request) {
  // TODO localize on request.languageCode,values cf. https://support.google.com/googleplay/android-developer/table/4419860
  var config = {
    configParams: [
      {
        type: 'INFO',
        name: 'instructions',
        text: 'Search for a nearby hackair home sensor on https://platform.hackair.eu/ or operate one yourself. Enter the sensorid here.'
      },
      {
        type: 'TEXTINPUT',
        name: 'sensorid',
        displayName: 'Enter a hackair home sensor sensorid',
        helpText: 'A sensorid consists usually of three digits',
        placeholder: 'sensorid',
        parameterControl: {
          allowOverride: true
        }
      },
      {
        type: "SELECT_SINGLE",
        name: "valueType",
        displayName: "Select value type (particle size)",
        helpText: "Use Dummy as a lead for data fusion",
        parameterControl: {
          allowOverride: true
        },
        options: [
          {
            label: "PM 10",
            value: PM10
          },
          {
            label: "PM2.5",
            value: PM25
          },
          {
            label: "Dummy for Fusion",
            value: "Dummy"
          }
        ]
      }      
    ],
    dateRangeRequired: true
  };
  return config;
}

var sensorSchema = [
  {
    name: 'value',
    dataType: 'NUMBER',
    semantics: {
      conceptType: 'METRIC',
      semanticType: 'NUMBER',
      isReaggregatable: true
    },
    defaultAggregationType: 'AVG'
  },
  {
    name: 'datetime',
    dataType: 'STRING',
    isDefault: true,
    semantics: {
      conceptType: 'DIMENSION',
      semanticType: 'YEAR_MONTH_DAY_HOUR'
    }
  },
  {
    name: 'date',
    dataType: 'STRING',
    semantics: {
      conceptType: 'DIMENSION',
      semanticType: 'YEAR_MONTH_DAY'
    }
  }
];

function getSchema(request) {
  return { schema: sensorSchema };
}

var cache = CacheService.getScriptCache();

function getData(request) {
  var getdataCalled = 'getdata called';
  console.log({message: getdataCalled, request: request});
  console.time(getdataCalled);
  // Create schema for requested fields
  var requestedSchema = request.fields.map(function (field) {
    for (var i = 0; i < sensorSchema.length; i++) {
      if (sensorSchema[i].name == field.name) {
        return sensorSchema[i];
      }
    }
  });
  var end = new Date(request.dateRange.endDate);
  var now = new Date();
  if (end > now) end = now;
  var hourDataAll = [];
  for (var currentStart = new Date(request.dateRange.startDate); currentStart <= end; currentStart.setDate(currentStart.getDate() + 1)) { // fetch day-wise because of 5000 limit per fetch
    var fnInitHourData = function() {
      var hourData = [];
      var dateMoment = Moment.moment(currentStart);
      for (var h = 0; h < 24; h++) {
        dateMoment.hour(h);
        hourData[h] = { datetime: dateMoment.format('YYYYMMDDHH'), sum: 0, count: 0 };
      }                                
      return hourData;
    };
    var hourData;
    if (request.configParams.valueType === 'Dummy') {
      hourData = fnInitHourData();
    } else {
      var currentEnd = new Date(currentStart.getTime());
      currentEnd.setDate(currentEnd.getDate() + 1);
      var csvUrl = "https://api.hackair.eu/measurements/export?sensor_id=" + request.configParams.sensorid + "&start=" + currentStart.toISOString() + "&end=" + currentEnd.toISOString();
      console.time(csvUrl);
      var fnCacheKey = function(sensorid, currentStart, valueType) {
        return sensorid + "|" + currentStart.toISOString() + "|" + valueType;
      };
      var cacheKey = fnCacheKey(request.configParams.sensorid, currentStart, request.configParams.valueType);
      var cacheSuccess = false;
      var cacheText = cache.get(cacheKey); // use cache
      if (cacheText) {
        try {
          hourData = JSON.parse(cacheText);
          if (!Array.isArray(hourData)) {
            throw "No array";
          }
          cacheSuccess = true;
          console.log('Using Cache for the URL "%s"', csvUrl);
        } catch(e) {
          console.log({message: 'JSON error', exception: e, string: cacheText});
          cache.remove(cacheKey);
        }        
      }
      if (!cacheSuccess) {
        console.log('Calling the URL "%s"', csvUrl);
        var csvContent = UrlFetchApp.fetch(csvUrl).getContentText();
        var csvData = Utilities.parseCsv(csvContent);
        csvData.shift(); // remove header line
        console.log('Received %d lines of data', csvData.length);
        // exemplary result:
        //726,sensors_arduino,"2018-09-15 07:17:00",PM2.5_AirPollutantValue,17.96,"micrograms/cubic meter",PM2.5_AirPollutantIndex,good,"48.186890411259,11.365576386452"
        //726,sensors_arduino,"2018-09-15 07:17:00",PM10_AirPollutantValue,18.83,"micrograms/cubic meter",PM10_AirPollutantIndex,"very good","48.186890411259,11.365576386452"      
        csvData = csvData.map(function(row){return [row[2], row[3], row[4]];});
        [PM10, PM25].forEach(function(valueType) {
          var hourDataCurrent = fnInitHourData();
          csvData.forEach(function(row) {
            // filter valueType and remove erroneous and out of day range entries
            if (row[1] === valueType && row[2] !== 'nan') {
              var value = Number(row[2]);
              if (value < 100) {
                var lineMoment = Moment.moment(row[0]);
                if (!lineMoment.isAfter(currentEnd) && !lineMoment.isBefore(currentStart)) {
                  var hour = lineMoment.hour();
                  hourDataCurrent[hour].sum += value;
                  hourDataCurrent[hour].count++;
                }
              }
            }
          });
          hourDataCurrent = hourDataCurrent.filter(function(data){ return data.count !== 0; });
          cacheKey = fnCacheKey(request.configParams.sensorid, currentStart, valueType);
          try {
            cache.put(cacheKey, JSON.stringify(hourDataCurrent), (end == now)?(10*60):(6*60*60)); // store in cache, for current day short validity of 10 min, otherwise infinite validity = max poassible= 6 hours     
          } catch(e) {
            console.log({message: 'JSON error', exception: e});
            cache.remove(cacheKey);
          }      
          if (valueType === request.configParams.valueType) {
            hourData = hourDataCurrent;
          }
        });
        console.log({message: 'parsed data', hourData: hourData});
      }
    }
    hourDataAll = hourDataAll.concat(hourData);
    console.timeEnd(csvUrl);
  }
  // filter for requested fields
  var requestedData = hourDataAll.map(function(data) {
    var values = [];
    requestedSchema.forEach(function (field) {
      switch (field.name) {
        case 'datetime':
          values.push(data.datetime);
          break;
        case 'date':
          values.push(data.datetime.substring(0,8));
          break;
        case 'value':
          values.push((data.count === 0)?0:(data.sum/data.count));
          break;
        default:
          values.push('');
      }
    });
    return { values: values };
  });
  
  console.log({message: 'getdata finished', requestedData_0: requestedData[0], requestedData_length: requestedData.length});
  console.timeEnd(getdataCalled);
  return {
    schema: requestedSchema,
    rows: requestedData
  };    
}
