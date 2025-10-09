
install spatial; load spatial; -- in order to play with geometries

use demo_ducklake;
USE my_ducklake;
show tables;

------------------------------------------------------
-- import bixi stations (Montreal's BikeShare 🚲)
------------------------------------------------------

drop table stations;

create table stations as
with sta_info as (
	select unnest("data".stations::json[]) station
	FROM 'https://gbfs.velobixi.com/gbfs/fr/station_information.json'
)
select
	station->>'short_name' as code,
	(station->>'capacity')::int as capacity,
	station->>'name' as name,
	ST_point((station->>'lon')::numeric, (station->>'lat')::numeric) station_geom,
	-- et si on ajoutait d'autres colonnes ?
from sta_info
where (station->>'lon')::int != 0
order by 1;

-- let's be curious on the intermediate step
	select unnest("data".stations::json[]) station
	FROM 'https://gbfs.velobixi.com/gbfs/fr/station_information.json';

from stations;

show tables;

-----------------------------------------------------
-- import municipal sectors from OD survey 2013
-----------------------------------------------------
create or replace table sectors as
with t as (
	select unnest(features) feat
    from read_json_auto('https://www.donneesquebec.ca/recherche/dataset/b57cdeb1-98e7-4db7-bb84-32530f0367eb/resource/95ab084b-727e-4322-9433-0fed7baa690d/download/artm-sm-od13.geojson', sample_size=-1)
)
select
    feat.properties.SM13::int sector_id,
    feat.properties.SM13_nom sector_name,
    ST_GeomFromGeoJSON(feat.geometry::json) sector_geom,
    ST_Centroid(sector_geom) sector_centroid,
from t;

from sectors;

------------------------------------------------
-- How many stations & docks per sector ??
------------------------------------------------
select
	sector_name,
	any_value(sector_geom) as sector_geom,
	count(1) nb_station,
	sum(capacity) capacity,
	ST_Union_Agg(station_geom) geom_stations
from stations
  left join sectors
  	on ST_Within(station_geom, sector_geom)
group by 1
order by 1



from '/home/***REMOVED***/Documents/codes/ducklake_demo/data_files/main/stations/ducklake-0199c467-d9d6-7804-8c67-2b01e49056ba.parquet'


