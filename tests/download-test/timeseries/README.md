# New API backend tests - Timeseries

Testing timeseries downloads.

## PCDS style downloads

### Test PCDS-01-prod

Location: Williams Lake ENV-AQN station

Downloaded using production app

`
https://data.pacificclimate.org/data/pcds/agg/?from-date=2019%2F10%2F01&to-date=2019%2F10%2F07&input-polygon=MULTIPOLYGON%28%28%28-122.14364164880482+52.172232245431694%2C-122.20625599459437+52.13071529609348%2C-122.10946606130169+52.12250946008742%2C-122.14364164880482+52.172232245431694%29%29%29&input-var=&network-name=&input-freq=&data-format=nc&cliptodate=cliptodate&download-timeseries=Timeseries
`

### Test PCDS-01-dev-00

Williams Lake ENV-AQN station

`
http://127.0.0.1:8000/data/pcds/agg/?from-date=2019%2F10%2F01&to-date=2019%2F10%2F07&input-polygon=MULTIPOLYGON%28%28%28-122.14813894528264+52.16740040655235%2C-122.20242182859448+52.128098220171715%2C-122.11520844556614+52.12643973548231%2C-122.14813894528264+52.16740040655235%29%29%29&input-var=&network-name=&input-freq=&data-format=nc&cliptodate=cliptodate&download-timeseries=Timeseries
`

Results match prod

## SDP style downloads

### Specifying networks

Location: North of Alexandria.

#### Test SDP-01-prod-app

Polygon selects stations (without network constraint) from each of the following networks:
- EC_raw
- ENV-AQN
- FLNRO-WMB
- MoTIe

Unmodified request URL:

`
https://data.pacificclimate.org/data/pcds/agg/?from-date=2019%2F10%2F01&to-date=2019%2F10%2F07&input-polygon=MULTIPOLYGON%28%28%28-122.67341867516737+53.13706416477885%2C-122.67156029748213+52.843356654542106%2C-121.77613649171914+52.87430745404717%2C-122.67341867516737+53.13706416477885%29%29%29&input-var=&network-name=&input-freq=&data-format=nc&cliptodate=cliptodate&download-timeseries=Timeseries
`

#### Test SDP-01-dev-00

`
http://127.0.0.1:8000/data/pcds/agg/?from-date=2019%2F10%2F01&to-date=2019%2F10%2F07&input-polygon=MULTIPOLYGON%28%28%28-122.64258742908558+53.10633104156565%2C-122.63741921684851+52.85731683917066%2C-121.73592170091042+52.86289168744239%2C-122.64258742908558+53.10633104156565%29%29%29&input-var=&network-name=&input-freq=&data-format=nc&cliptodate=cliptodate&download-timeseries=Timeseries
`

Same stations downloaded

File sizes match

#### Test SDP-01-dev-01

Based on SDP-01-dev-00

Select two networks, EC_raw, MoTIe

Modified request URL:

`
http://127.0.0.1:8000/data/pcds/agg/?from-date=2019%2F10%2F01&to-date=2019%2F10%2F07&input-polygon=MULTIPOLYGON%28%28%28-122.64258742908558+53.10633104156565%2C-122.63741921684851+52.85731683917066%2C-121.73592170091042+52.86289168744239%2C-122.64258742908558+53.10633104156565%29%29%29&input-var=&network-name=EC_raw%2CMoTIe&input-freq=&data-format=nc&cliptodate=cliptodate&download-timeseries=Timeseries
`

Downloaded only EC_raw, MoTIE stations

File sizes match

#### Test SDP-01-dev-02

Based on SDP-01-dev-00
Select one network, ENV-AQN

Modified request URL:

`
http://127.0.0.1:8000/data/pcds/agg/?from-date=2019%2F10%2F01&to-date=2019%2F10%2F07&input-polygon=MULTIPOLYGON%28%28%28-122.64258742908558+53.10633104156565%2C-122.63741921684851+52.85731683917066%2C-121.73592170091042+52.86289168744239%2C-122.64258742908558+53.10633104156565%29%29%29&input-var=&network-name=ENV-AQN&input-freq=&data-format=nc&cliptodate=cliptodate&download-timeseries=Timeseries
`

Downloaded only ENV-AQN stations
File sizes match

### Specifying observation frequencies

Location: Penticton area.

Polygon selects 2 stations (without frequency constraint), one from each of the following networks:
- EC_raw
- FLNRO-WMB

Frequencies:
- Hourly (FLNRO-WMB)
- Daily (EC_raw)

#### Test SDP-02-prod-app

Unmodified request URL:
`
https://data.pacificclimate.org/data/pcds/agg/?from-date=2019%2F10%2F01&to-date=2019%2F10%2F07&input-polygon=MULTIPOLYGON%28%28%28-119.67777625913594+49.59013205038748%2C-119.64086598556392+49.49529498760836%2C-119.47562281413748+49.5092632542256%2C-119.67777625913594+49.59013205038748%29%29%29&input-var=&network-name=&input-freq=&data-format=nc&cliptodate=cliptodate&download-timeseries=Timeseries
`

#### Test SDP-02-dev-00

`
http://127.0.0.1:8000/data/pcds/agg/?from-date=2019%2F10%2F01&to-date=2019%2F10%2F07&input-polygon=MULTIPOLYGON%28%28%28-119.67777625913594+49.59013205038748%2C-119.64086598556392+49.49529498760836%2C-119.47562281413748+49.5092632542256%2C-119.67777625913594+49.59013205038748%29%29%29&input-var=&network-name=&input-freq=&data-format=nc&cliptodate=cliptodate&download-timeseries=Timeseries
`

Downloaded same stations as prod
File sizes match

#### Test SDP-02-dev-01

Based on SDP-02-dev-00
Select 1-hourly frequency

`
http://127.0.0.1:8000/data/pcds/agg/?from-date=2019%2F10%2F01&to-date=2019%2F10%2F07&input-polygon=MULTIPOLYGON%28%28%28-119.67777625913594+49.59013205038748%2C-119.64086598556392+49.49529498760836%2C-119.47562281413748+49.5092632542256%2C-119.67777625913594+49.59013205038748%29%29%29&input-var=&network-name=&input-freq=1-hourly&data-format=nc&cliptodate=cliptodate&download-timeseries=Timeseries
`

Downloaded only FLNRO-WMB station
File size matches

#### Test SDP-02-dev-02

Based on SDP-02-dev-00
Select daily frequency

`
http://127.0.0.1:8000/data/pcds/agg/?from-date=2019%2F10%2F01&to-date=2019%2F10%2F07&input-polygon=MULTIPOLYGON%28%28%28-119.67777625913594+49.59013205038748%2C-119.64086598556392+49.49529498760836%2C-119.47562281413748+49.5092632542256%2C-119.67777625913594+49.59013205038748%29%29%29&input-var=&network-name=&input-freq=daily&data-format=nc&cliptodate=cliptodate&download-timeseries=Timeseries
`

Downloaded only EC_raw station
File size matches

#### Test SDP-02-dev-03

Based on SDP-02-dev-00
Select 1-hourly,daily frequency

`
http://127.0.0.1:8000/data/pcds/agg/?from-date=2019%2F10%2F01&to-date=2019%2F10%2F07&input-polygon=MULTIPOLYGON%28%28%28-119.67777625913594+49.59013205038748%2C-119.64086598556392+49.49529498760836%2C-119.47562281413748+49.5092632542256%2C-119.67777625913594+49.59013205038748%29%29%29&input-var=&network-name=&input-freq=1-hourly%2Cdaily&data-format=nc&cliptodate=cliptodate&download-timeseries=Timeseries
`

Downloaded both stations
File sizes matches

#### Test SDP-02-dev-04

Based on SDP-02-dev-00
Select 1-hourly,daily,foobar frequency

`
http://127.0.0.1:8000/data/pcds/agg/?from-date=2019%2F10%2F01&to-date=2019%2F10%2F07&input-polygon=MULTIPOLYGON%28%28%28-119.67777625913594+49.59013205038748%2C-119.64086598556392+49.49529498760836%2C-119.47562281413748+49.5092632542256%2C-119.67777625913594+49.59013205038748%29%29%29&input-var=&network-name=&input-freq=1-hourly%2Cdaily%2Cfoobar&data-format=nc&cliptodate=cliptodate&download-timeseries=Timeseries
`

Downloaded both stations (and no others)
File sizes match
Is this expected? See next test. 
Possibly a partial match works, i.e., one matching frequency is sufficient to enable the filter. That's not very intuitive. Easiest fix would be to allow any frequency values.

#### Test SDP-02-dev-05

Based on SDP-02-dev-00
Select foobar frequency (invalid criterion)

`
http://127.0.0.1:8000/data/pcds/agg/?from-date=2019%2F10%2F01&to-date=2019%2F10%2F07&input-polygon=MULTIPOLYGON%28%28%28-119.67777625913594+49.59013205038748%2C-119.64086598556392+49.49529498760836%2C-119.47562281413748+49.5092632542256%2C-119.67777625913594+49.59013205038748%29%29%29&input-var=&network-name=&input-freq=foobar&data-format=nc&cliptodate=cliptodate&download-timeseries=Timeseries
`

Nothing downloaded
Is this expected? I thought it was supposed to omit any filter that does not match the pattern. The pattern here is explicit about allowed frequency names. So this should omitted freq filter, and therefore downloaded both stations. WTF?

#### Test SDP-02-dev-06

Based on SDP-02-dev-00
Select irregular frequency

`
http://127.0.0.1:8000/data/pcds/agg/?from-date=2019%2F10%2F01&to-date=2019%2F10%2F07&input-polygon=MULTIPOLYGON%28%28%28-119.67777625913594+49.59013205038748%2C-119.64086598556392+49.49529498760836%2C-119.47562281413748+49.5092632542256%2C-119.67777625913594+49.59013205038748%29%29%29&input-var=&network-name=&input-freq=irregular&data-format=nc&cliptodate=cliptodate&download-timeseries=Timeseries
`

Nothing downloaded
As expected: There are no stations matching this valid criterion

### Specifying variables

Location: Fort St John area N to Rose Prairie

Polygon selects 5 stations in the following networks, stations (native_id), variables:
- ENV-AQN
  - E304453: 
    - Temperature (Mean), 
    - Relative Humidity (Point), 
    - Air Pressure (Point)
  - 0770708: 
    - Wind Speed (Point), 
    - Temperature (Mean), 
    - Relative Humidity (Point), 
    - Wind Direction (Point)
  - E234230: 
    - Wind Speed (Point), 
    - Temperature (Mean), 
    - Relative Humidity (Point), 
    - Wind Direction (Point)
- FLNRO-WMB
  - 1045: 
    - Temperature (Point), 
    - Precipitation Amount, 
    - Wind Speed (Mean), 
    - Wind Direction (Mean), 
    - Relative Humidity (Mean)
- MoTIe 
  - 44091:
    - Rainfall Amount
    - Temperature (Min.)
    - Surface Snow Depth (Point)
    - Wind Speed (Max.)
    - Wind Speed (Point)
    - Precipitation Amount
    - Precipitation (Cumulative)
    - Relative Humidity (Mean)
    - Temperature (Point)
    - Wind Direction (Mean)
    - Snowfall Amount
    - Dew Point Temperature (Mean)
    - Wind Direction (Point)
    - Precipitation Amount
    - Temperature (Point)
    - Wind Direction (Std Dev)
    - Wind Speed (Mean)
    - Temperature (Max.)
    - Air Pressure (Point) 

Variables (PCDS selector):
- Precipitation Amount (1045, 44091): `lwe_thickness_of_precipitation_amount_sum`
- Temperature (Mean) (E304453, 0770708, E234230): `air_temperature_mean`

#### Test SDP-03-prod-app

`
https://data.pacificclimate.org/data/pcds/agg/?from-date=2019%2F10%2F01&to-date=2019%2F10%2F08&input-polygon=MULTIPOLYGON%28%28%28-121.39162801903346+56.61613720987384%2C-121.52603934226993+56.1975595175515%2C-120.57231402150325+56.05081187111317%2C-120.27893985800799+56.59140037350869%2C-121.39162801903346+56.61613720987384%29%29%29&input-var=&network-name=&input-freq=&data-format=nc&cliptodate=cliptodate&download-timeseries=Timeseries
`

Downloads expected stations


#### Test SDP-03-dev-00

`
http://127.0.0.1:8000/data/pcds/agg/?from-date=2019%2F10%2F01&to-date=2019%2F10%2F08&input-polygon=MULTIPOLYGON%28%28%28-121.39162801903346+56.61613720987384%2C-121.52603934226993+56.1975595175515%2C-120.57231402150325+56.05081187111317%2C-120.27893985800799+56.59140037350869%2C-121.39162801903346+56.61613720987384%29%29%29&input-var=&network-name=&input-freq=&data-format=nc&cliptodate=cliptodate&download-timeseries=Timeseries
`

Downloads expected files
File sizes match

#### Test prologue

Need to look up the `vars` lists for some of the stations.

```
select vars
from crmp.crmp_network_geoserver
where native_id = 'xxxx'
```

- E304453: "air_temperature_mean, relative_humidity_point, air_pressure_point"
- 0770708: "wind_speed_point, air_temperature_mean, relative_humidity_point, wind_from_direction_point"
- E234230: "wind_speed_point, air_temperature_mean, relative_humidity_point, wind_from_direction_point"
- 1045: "air_temperature_point, lwe_thickness_of_precipitation_amount_sum, wind_speed_mean, wind_from_direction_mean, relative_humidity_mean"
- 44091: 
  - "air_temperature_minimum"
  - "surface_snow_thickness_point"
  - "wind_speed_maximum"
  - "wind_speed_point"
  - "lwe_thickness_of_precipitation_amountt: sum within days interval: daily"
  - "lwe_thickness_of_precipitation_amountt: sum over days interval: irregular"
  - "relative_humidity_mean"
  - "air_temperature_point"
  - "wind_from_direction_mean"
  - "thickness_of_snowfall_amount_sum"
  - "dew_point_temperature_mean"
  - "wind_from_direction_point"
  - "lwe_thickness_of_precipitation_amount_sum"
  - "air_temperature_point"
  - "wind_from_direction_standard_deviation"
  - "wind_speed_mean"
  - "air_temperature_maximum"
  - "air_pressure_point"


#### Test SDP-03-dev-01

Based on SDP-03-dev-00
One variable, foobar (not present)

`
http://127.0.0.1:8000/data/pcds/agg/?from-date=2019%2F10%2F01&to-date=2019%2F10%2F08&input-polygon=MULTIPOLYGON%28%28%28-121.39162801903346+56.61613720987384%2C-121.52603934226993+56.1975595175515%2C-120.57231402150325+56.05081187111317%2C-120.27893985800799+56.59140037350869%2C-121.39162801903346+56.61613720987384%29%29%29&input-vars=foobar&network-name=&input-freq=&data-format=nc&cliptodate=cliptodate&download-timeseries=Timeseries
`

Nothing downloaded
As expected

#### Test SDP-03-dev-02

Based on SDP-03-dev-00
One variable, air_temperature_mean (present)

`
http://127.0.0.1:8000/data/pcds/agg/?from-date=2019%2F10%2F01&to-date=2019%2F10%2F08&input-polygon=MULTIPOLYGON%28%28%28-121.39162801903346+56.61613720987384%2C-121.52603934226993+56.1975595175515%2C-120.57231402150325+56.05081187111317%2C-120.27893985800799+56.59140037350869%2C-121.39162801903346+56.61613720987384%29%29%29&input-vars=air_temperature_mean&network-name=&input-freq=&data-format=nc&cliptodate=cliptodate&download-timeseries=Timeseries
`

Stations downloaded: 0770708, E234230, E304453
As expected
File sizes match

#### Test SDP-03-dev-03

Based on SDP-03-dev-00
One variable, air_temperature_mean (present), foobar (not present)

`
http://127.0.0.1:8000/data/pcds/agg/?from-date=2019%2F10%2F01&to-date=2019%2F10%2F08&input-polygon=MULTIPOLYGON%28%28%28-121.39162801903346+56.61613720987384%2C-121.52603934226993+56.1975595175515%2C-120.57231402150325+56.05081187111317%2C-120.27893985800799+56.59140037350869%2C-121.39162801903346+56.61613720987384%29%29%29&input-vars=air_temperature_mean%2Cfoobar&network-name=&input-freq=&data-format=nc&cliptodate=cliptodate&download-timeseries=Timeseries
`

Stations downloaded: 0770708, E234230, E304453
As expected
File sizes match

#### Test SDP-03-dev-04

Based on SDP-03-dev-00
One variable, air_temperature_mean (present), wind_from_direction_mean (present)

`
http://127.0.0.1:8000/data/pcds/agg/?from-date=2019%2F10%2F01&to-date=2019%2F10%2F08&input-polygon=MULTIPOLYGON%28%28%28-121.39162801903346+56.61613720987384%2C-121.52603934226993+56.1975595175515%2C-120.57231402150325+56.05081187111317%2C-120.27893985800799+56.59140037350869%2C-121.39162801903346+56.61613720987384%29%29%29&input-vars=air_temperature_mean%2Cwind_from_direction_mean&network-name=&input-freq=&data-format=nc&cliptodate=cliptodate&download-timeseries=Timeseries
`

Stations downloaded: all
As expected
File sizes match

#### Test SDP-03-dev-05

Based on SDP-03-dev-00
One variable, relative_humidity_mean, wind_from_direction_mean, wind_speed_point

`
http://127.0.0.1:8000/data/pcds/agg/?from-date=2019%2F10%2F01&to-date=2019%2F10%2F08&input-polygon=MULTIPOLYGON%28%28%28-121.39162801903346+56.61613720987384%2C-121.52603934226993+56.1975595175515%2C-120.57231402150325+56.05081187111317%2C-120.27893985800799+56.59140037350869%2C-121.39162801903346+56.61613720987384%29%29%29&input-vars=relative_humidity_mean%2Cwind_from_direction_mean%2Cwind_speed_point&network-name=&input-freq=&data-format=nc&cliptodate=cliptodate&download-timeseries=Timeseries
`

Stations downloaded: all but E304453
As expected
File sizes match
