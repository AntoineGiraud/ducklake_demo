--------------------------------------------------------------------------------
-- démo cnx à S3
--> [doc](https://duckdb.org/docs/stable/core_extensions/httpfs/s3api)
--------------------------------------------------------------------------------

CREATE or replace secret(
    TYPE s3,
    PROVIDER config,
    KEY_ID 'xxxxxx',
    SECRET 'yyyyyyyyyyyy',
	-- ENDPOINT 'url_de_mon_s3_prive'
);

-- la liste des fichiers présents ??
FROM glob('s3://mon_bucket/**/*');
-- à quoi ressemble le contenu du fichier ?
summarize 's3://mon_bucket/ducklake*.parquet';

--------------------------------------------------------------------------------
-- démo copie d'un fichier d'un S3 à un autre
--------------------------------------------------------------------------------

CREATE or replace secret dev_s3(
    TYPE s3,
    PROVIDER config,
    KEY_ID 'xxxxxx',
    SECRET 'yyyyyyyyyyyy',
	-- ENDPOINT 'url_de_mon_s3_prive',
	SCOPE 's3://mon_bucket'
);

-- ok :)
FROM glob('s3://mon_bucket/**/*');
-- ko car pas de secret yet
FROM glob('s3://mon_2nd_bucket/JAFFLE/**/*');

CREATE or replace secret jaffle_s3(
    TYPE s3,
    PROVIDER config,
    KEY_ID 'xxxxxx',
    SECRET 'yyyyyyyyyyyy',
	-- ENDPOINT 'url_de_mon_s3_prive',
	SCOPE 's3://mon_2nd_bucket'
);

-- ok :)
FROM glob('s3://mon_2nd_bucket/JAFFLE/**/*');

-- qu'avons nous là ?
from 's3://mon_2nd_bucket/JAFFLE/ORDER/raw_stores.csv';

-- on le rappatrie ?
copy (from 's3://mon_2nd_bucket/JAFFLE/ORDER/raw_stores.csv')
to 's3://mon_bucket/jaffle/raw_stores.parquet';
-- ça a fonctionné ?
FROM glob('s3://mon_bucket/**/*');


--------------------------------------------------------------------------------
-- old way -> privilégier les secrets
--------------------------------------------------------------------------------

INSTALL aws; LOAD aws;
INSTALL httpfs; LOAD httpfs;

-- pas forcément nécessaire
--SET ca_cert_file = '~/orange-certificates.crt';
--SET enable_server_cert_verification = true;
 
-- SET s3_region = 'us-east-1';
-- SET s3_endpoint = 'url_de_mon_s3_prive';
SET s3_use_ssl='true';
SET s3_url_style='path';
SET s3_access_key_id='xxxxxx';
SET s3_secret_access_key='yyyyyyyyyyyy';
 
 
FROM glob('s3://mon_bucket/*.parquet');



