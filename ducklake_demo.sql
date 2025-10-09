-- duckdb sql

-------------------------------------
-- connect to catalog & ducklake
--> ducklake [manifesto](https://ducklake.select/manifesto/)
-------------------------------------

INSTALL ducklake;
INSTALL postgres;

CREATE SECRET (
    TYPE postgres,
    HOST 'localhost',
    PORT 5432,
    DATABASE ducklake_catalog,
    USER 'duckuser',
    PASSWORD 'duckpass'
);

ATTACH 'ducklake:postgres:dbname=ducklake_catalog host=localhost' AS my_ducklake
(
	DATA_PATH '/home/agiraud/Documents/codes/ducklake_demo/data_files/',
    -- le ~ à la place de /home/mon_user/ ne fonctionne pas ici ... qd on est dans DBeaver
    -- d'ailleurs, si depuis duckdb en cli `DATA_PATH 'data_files'` fonctionne très bien
	OVERRIDE_DATA_PATH True
);
-- use demo_ducklake; detach my_ducklake;
USE my_ducklake;

show tables;

-------------------------------------
-- play with some data
-------------------------------------
CREATE or replace TABLE nl_train_stations as
  FROM 'https://blobs.duckdb.org/nl_stations.csv';

-- à quoi ressemble notre db / catalogue ?
show tables;
from nl_train_stations limit 10;
summarize nl_train_stations;

-- à quoi ressemble le(s) fichiers ?
FROM glob('~/Documents/codes/ducklake_demo/data_files/**/*');
FROM '~/Documents/codes/ducklake_demo/data_files/**/*.parquet' LIMIT 10;

----------------------------
-- allez un p'tit update
----------------------------
UPDATE nl_train_stations SET name_long = 'Johan Cruijff ArenA' WHERE code = 'ASB';
-- ça fonctionne ?
SELECT name_long FROM nl_train_stations WHERE code = 'ASB';

-- que sont devenus les fichiers ?
FROM glob('ducklake_demo/data_files/**/*');
    --> le .parquet original
    ---> un .parquet avec la nouvelle valeur 
    from '~/Documents/codes/ducklake_demo/data_files/main/nl_train_stations/ducklake-0199c45d-2303-73bf-b40a-61b9f9ee82c1.parquet';
    ---> un .parquet avec la ligne deleted (ancienne valeur)
    from '~/Documents/codes/ducklake_demo/data_files/main/nl_train_stations/*-delete.parquet'; 

-------------------------------------
-- TimeTravel vous avez dit ??
--> [doc](https://ducklake.select/docs/stable/duckdb/usage/time_travel)
-------------------------------------

-- récap des snapshots
FROM my_ducklake.snapshots();

-- avant modif
SELECT name_long
FROM nl_train_stations AT (VERSION => 1)
WHERE code = 'ASB';

-- apres modif
SELECT name_long
FROM nl_train_stations AT (VERSION => 2)
WHERE code = 'ASB';

-- apres modif version timestamp
SELECT name_long
FROM nl_train_stations AT (TIMESTAMP => now() - INTERVAL '1 minute')
WHERE code = 'ASB';


-------------------------------------
-- Du ménage ??
-------------------------------------

CALL ducklake_rewrite_data_files('my_ducklake');

CALL ducklake_cleanup_old_files(
    'my_ducklake',
    cleanup_all => true
);

CALL ducklake_delete_orphaned_files(
    'my_ducklake',
    older_than => now() - INTERVAL '5 minute'
);

