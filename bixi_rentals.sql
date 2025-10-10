
use demo_ducklake;
USE my_ducklake;
show tables;

----------------------------------------
-- let's inspect bixi's 2020 rentals
----------------------------------------
create table demo_ducklake.main.rentals_2020 as -- ~21 Mo parquet with all 2020 rentals
from 'hf://datasets/antoinegiraud/bixi_opendata/rentals_2020.parquet';

-- note: instantané depuis un fichier local
-- ... très très lent depuis un fichier distant hf:// directement dans DuckLake...
create table my_ducklake.main.rentals_2020 as from demo_ducklake.main.rentals_2020;

summarize rentals_2020; -- récap per colomn : min / max / count / distinct count

-- daily records (per year)
select
	year(start_date) as annee,
	start_date,
	count(1) as rentals,
	row_number() over (partition by annee order by rentals desc) as rang
from rentals_2020
group by 1,2
qualify rang < 3
order by 1 desc, rang;

-- let's offload the data in various formats
copy rentals_2020 to '~/Documents/rentals_2020.parquet';
copy rentals_2020 to '~/Documents/rentals_2020.csv';
copy rentals_2020 to '~/Documents/rentals_2020.json';
-- note : '~/' == 'C:\Users\agiraud/'

-- offload en partitions date ?!!
COPY rentals_2020 TO '~/Documents/rentals_2020_partitions/'
(FORMAT parquet, PARTITION_BY (start_date), OVERWRITE True);

-- à quoi ressemblent les fichiers générés ?!
FROM glob('~/Documents/rentals_2020_partitions/**/*');


----------------------------------------
-- offload to s3
----------------------------------------
use demo_ducklake; -- bdd duckdb

CREATE or replace secret(
    TYPE s3,
    PROVIDER config,
    KEY_ID 'xxxxx',
    SECRET 'xxxxxxxx',
	-- ENDPOINT 'url_s3_prive'
);

copy rentals_2020 to 's3://mon_bucket/rentals_2020.parquet';

-- offload en partitions date ?!!
COPY rentals_2020 TO 's3://mon_bucket/rentals_2020_partitions/'
(FORMAT parquet, PARTITION_BY (start_date));
-- OVERWRITE is not supported for remote file systems

-- la liste des fichiers présents ??
FROM glob('s3://mon_bucket/**/*');

