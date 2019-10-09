# New API backend tests - Climatologies

Testing climatologies downloads.

## PCDS style downloads

### Test PCDS-01-prod

Location: 150 Mile House S. to Wright

Only climatology stations

2 stations: 
- 225 (FLNRO-WMB)
- 47092 (MoTIe)

Unmodified URL:

`
https://data.pacificclimate.org/data/pcds/agg/?from-date=2019%2F01%2F01&to-date=2019%2F10%2F08&input-polygon=MULTIPOLYGON%28%28%28-122.04142562395886+52.066627328384634%2C-121.76034536905189+52.1189207367513%2C-121.75364376441757+51.89687599986855%2C-122.04142562395886+52.066627328384634%29%29%29&input-var=&network-name=&input-freq=&only-with-climatology=only-with-climatology&data-format=nc&cliptodate=cliptodate&download-climatology=Climatology
`

Downloaded the two stations

### Test PCDS-01-dev-00

Based on PCDS-01-prod

`
http://127.0.0.1:8000/data/pcds/agg/?from-date=2019%2F01%2F01&to-date=2019%2F10%2F08&input-polygon=MULTIPOLYGON%28%28%28-122.04142562395886+52.066627328384634%2C-121.76034536905189+52.1189207367513%2C-121.75364376441757+51.89687599986855%2C-122.04142562395886+52.066627328384634%29%29%29&input-var=&network-name=&input-freq=&only-with-climatology=only-with-climatology&data-format=nc&cliptodate=cliptodate&download-climatology=Climatology
`

Downloaded the two stations

File sizes match

### Test PCDS-02-prod

Location: 150 Mile House and Williams Lake

Climatology and non-climatology stations

3 stations:
- 0550502 (ENV-AQN, non-climo)
- E248623 (ENV-AQN, non-climo)
- 225 (FLNRO-WMB, climo)

`
https://data.pacificclimate.org/data/pcds/agg/?from-date=2018%2F01%2F01&to-date=2019%2F10%2F08&input-polygon=MULTIPOLYGON%28%28%28-122.18064834322816+52.16786435012635%2C-122.18687958721628+52.097077322921244%2C-121.76272365196212+52.03242280045823%2C-122.18064834322816+52.16786435012635%29%29%29&input-var=&network-name=&input-freq=&data-format=nc&cliptodate=cliptodate&download-climatology=Climatology
`

One file (225.nc) containing netcdf data.

Two files (0550502.nc, E248623.nc) containing text error message.

As expected.

### Test PCDS-02-dev-00

Based on PCDS-02-prod

`
http://127.0.0.1:8000/data/pcds/agg/?from-date=2018%2F01%2F01&to-date=2019%2F10%2F08&input-polygon=MULTIPOLYGON%28%28%28-122.18064834322816+52.16786435012635%2C-122.18687958721628+52.097077322921244%2C-121.76272365196212+52.03242280045823%2C-122.18064834322816+52.16786435012635%29%29%29&input-var=&network-name=&input-freq=&data-format=nc&cliptodate=cliptodate&download-climatology=Climatology
`

Same files as PCDS-02-prod

File sizes match


## SDP style downloads

### Specifying networks

Check that network list works

#### Test SDP-01-dev-01

Based on PCDS-01-dev-00

Specify two networks: ENV-AQN (not present), FLNRO-WMB (present)

`
http://127.0.0.1:8000/data/pcds/agg/?from-date=2019%2F01%2F01&to-date=2019%2F10%2F08&input-polygon=MULTIPOLYGON%28%28%28-122.04142562395886+52.066627328384634%2C-121.76034536905189+52.1189207367513%2C-121.75364376441757+51.89687599986855%2C-122.04142562395886+52.066627328384634%29%29%29&input-var=&network-name=ENV-AQN%2CFLNRO-WMB&input-freq=&only-with-climatology=only-with-climatology&data-format=nc&cliptodate=cliptodate&download-climatology=Climatology
`

Downloaded only  FLNRO-WMB station

As expected

File sizes match

#### Test SDP-01-dev-02

Based on PCDS-01-dev-00

Specify two networks: MoTIe (present), FLNRO-WMB (present)

`
http://127.0.0.1:8000/data/pcds/agg/?from-date=2019%2F01%2F01&to-date=2019%2F10%2F08&input-polygon=MULTIPOLYGON%28%28%28-122.04142562395886+52.066627328384634%2C-121.76034536905189+52.1189207367513%2C-121.75364376441757+51.89687599986855%2C-122.04142562395886+52.066627328384634%29%29%29&input-var=&network-name=MoTIe%2CFLNRO-WMB&input-freq=&only-with-climatology=only-with-climatology&data-format=nc&cliptodate=cliptodate&download-climatology=Climatology
`

Downloaded both stations

As expected

File sizes match

## End

This is sufficient to demonstrate correctness on climatologies, given correctness on timeseries.







