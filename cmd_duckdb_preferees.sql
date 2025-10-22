--------------------------------------------------------------------
-- Mes commandes préférées
--------------------------------------------------------------------

-- lister les fichiers d'un dossier
from glob('~/Documents/rentals_2020/*');
-- lister les fichiers d'un s3
from glob('s3://mon_bucket/**/*');

-- matérialiser la table en local
create or replace table rentals_2020 as
from 'hf://datasets/antoinegiraud/bixi_opendata/rentals_2020.parquet';

-- lister les tables
show tables;
show all tables;

-- récap min/max
summarize rentals_2020;
-- afficher le plan d'exécution
explain summarize rentals_2020;
-- lister les colonnes
describe rentals_2020;

-- export des données
copy rentals_2020 to '~/Documents/rentals_2020/rentals_2020.parquet';
copy rentals_2020 to '~/Documents/rentals_2020/rentals_2020.csv';
copy rentals_2020 to '~/Documents/rentals_2020/rentals_2020.json';
-- export en partition
COPY rentals_2020 TO '~/Documents/rentals_2020/rentals_2020_partitions/' (FORMAT parquet, PARTITION_BY (start_date_month));
-- export d'une requête
copy (
	from rentals_2020 where start_date_month = '2020-04-01'
) to '~/Documents/rentals_2020/rentals_2020_01.parquet';


-----------------------------------------------------------------------
-- secrets
-----------------------------------------------------------------------
CREATE SECRET (
    TYPE postgres,
    HOST 'localhost',
    PORT 5432,
    DATABASE ducklake_catalog,
    USER 'duckuser',
    PASSWORD 'duckpass'
);
create or replace secret dev_s3(
    TYPE s3,
    PROVIDER config,
    KEY_ID 'xxxx',
    SECRET 'xxxxxxxxx',
    -- session_token 'xxxx', -- pour aws
    -- REGION 'eu-west-3' -- pour aws
	-- ENDPOINT 'url_de_mon_bucket_prive',
	SCOPE 's3://premier-bucket' -- limiter le secret à ce bucket
);

-----------------------------------------------------------------------
-- ducklake
-----------------------------------------------------------------------
ATTACH 'ducklake:postgres:dbname=ducklake_catalog_s3'
AS my_s3_ducklake (
	DATA_PATH 's3://mon_bucket/demo_ducklake/' -- surtout pour l'initialisation du catalogue, optionnel après
);
-- use local_duckdb; detach my_s3_ducklake; --> pour revenir sur le duckdb local
USE my_s3_ducklake;

show all tables;
show tables from my_s3_ducklake;

-----------------------------------------------------------------------
-- merge into
-----------------------------------------------------------------------
set VARIABLE path_vers_mes_fichiers= '~/Documents/rentals_2020';
SELECT getvariable('path_vers_mes_fichiers');

from glob(getvariable('path_vers_mes_fichiers') || '/**/*');

-- requêtons un fichier voir
select filename, *
from read_parquet(getvariable('path_vers_mes_fichiers') || '/*_partitions/*2020-04-01*/*.parquet')

create schema if not exists mon_domaine_metier;

-- 1er full load
create or replace table mon_domaine_metier.rentals__incremental as
from read_parquet(getvariable('path_vers_mes_fichiers') || '/*_partitions/*2020-04-01*/*.parquet');

-- work in progress
MERGE into mon_domaine_metier.rentals__incremental as cible
    USING (
		from read_parquet(getvariable('path_vers_mes_fichiers') || '/*_partitions/*2020-05-01*/*.parquet')
		--from read_parquet(getvariable('path_vers_mes_fichiers') || '/*_partitions/*2020-06-01*/*.parquet')
		--from read_parquet(getvariable('path_vers_mes_fichiers') || '/*_partitions/*2020-07-01*/*.parquet')
		qualify 1=row_number() over(partition by id_ent order by dmaj desc) --> doublons src ?
    ) maj
    USING (id_ent)
    -- when matched then update -> bourrin, on remet les données à chaque fois
    WHEN matched and ( --> si on rejoue, les données sont déjà là, il ne se passer rien
    	-- maj.dmaj > cible.dmaj --> on ne fait finalement pas confiance à la source
    	maj.date_part > cible.date_part --> pleins de données ne changent même pas ...
    	--and sha1(concat(cible.cod_cat_client, '|', cible.reserve_1, '|', cible.reserve_2))
    	--  !=sha1(concat(maj.cod_cat_client, '|', maj.reserve_1, '|', maj.reserve_2))
      ) THEN UPDATE
    WHEN NOT MATCHED THEN INSERT
    -- WHEN NOT MATCHED BY SOURCE THEN DELETE
  RETURNING merge_action, *;
