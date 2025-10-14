

-------------------------------------------------------
-- Secret pour PostgreSQL
-------------------------------------------------------

CREATE SECRET (
    TYPE postgres,
    HOST 'localhost',
    PORT 5432,
    DATABASE ducklake_catalog,
    USER 'duckuser',
    PASSWORD 'duckpass'
);

-------------------------------------------------------
-- Secrets pour les S3
-------------------------------------------------------

-- en mode générique
CREATE or replace secret(
    TYPE s3,
    PROVIDER config,
    KEY_ID 'xxxx',
    SECRET 'xxxxxxxxx',
	-- ENDPOINT 'url_de_mon_bucket_prive'
);

-- en mode restreint sur 1 seul bucket
CREATE or replace secret dev_s3(
    TYPE s3,
    PROVIDER config,
    KEY_ID 'xxxx',
    SECRET 'xxxxxxxxx',
	-- ENDPOINT 'url_de_mon_bucket_prive',
	SCOPE 's3://premier-bucket'
);

-- en mode restreint sur 1 seul bucket autre
CREATE or replace secret jaffle_s3(
    TYPE s3,
    PROVIDER config,
    KEY_ID 'yyy',
    SECRET 'yyyyyyyy',
	-- ENDPOINT 'url_de_mon_bucket_prive',
	SCOPE 's3://second-bucket'
);