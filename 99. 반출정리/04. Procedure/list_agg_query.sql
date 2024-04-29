select ordinal_position 
  from information_schema.columns
 where table_schema = 'dept_dw'
   and table_name = 'scm_cmmn_cd'
   and column_name = 'reg_dtm'
;
select --isc.column_name
       LISTAGG(isc.column_name, ', ') WITHIN GROUP (ORDER BY isc.ordinal_position) AS attname_list
  from information_schema.columns isc
  join pg_catalog.pg_stat_all_tables pgt
    on pgt.schemaname = isc.table_schema
   and pgt.relname = isc.table_name
  join pg_catalog.pg_attribute pga
    on pgt.relid = pga.attrelid
   and isc.column_name = pga.attname
 where isc.table_name = 'scm_cmmn_cd'
   and pga.attnum < 5
;
select pga.attname 
--LISTAGG(pga.attname, ', ') WITHIN GROUP (ORDER BY pga.attnum) AS attname_list
  FROM pg_catalog.pg_attribute pga
  JOIN pg_catalog.pg_stat_all_tables pgt
    ON pgt.relid = pga.attrelid
 WHERE pgt.relname = 'scm_cmmn_cd'
   AND pga.attnum > 0
   AND pga.attnum < 5
-- GROUP BY pgt.relname
;
SELECT STRING_AGG(pga.attname, ', ') AS attname_list
FROM pg_catalog.pg_attribute pga
JOIN pg_catalog.pg_stat_all_tables pgt ON pgt.relid = pga.attrelid
WHERE pgt.relname = 'scm_cmmn_cd'
  AND pga.attnum > 0
  AND pga.attnum < 5
GROUP BY pgt.relname
;
select count(1) from (
select str_cd,shop_cd,md_cd
  from dept_dw.smt_shop_md_m
 group by str_cd,shop_cd,md_cd
having count(1) > 1
)
;
    SELECT isc.column_name as col_name
         , pga.attnum as ordinal_position
    FROM information_schema.columns isc
    JOIN pg_catalog.pg_stat_all_tables pgt ON pgt.schemaname = isc.table_schema
      AND pgt.relname = isc.table_name
    JOIN pg_catalog.pg_attribute pga ON pgt.relid = pga.attrelid
      AND isc.column_name = pga.attname
    WHERE isc.table_name = 'scm_cmmn_cd'
      AND pga.attnum < 2
;
select LISTAGG(sc.column_name, ', ') WITHIN GROUP (ORDER BY sc.ordinal_position) AS attname_list
  from pg_catalog.svv_table_info sti 
  join pg_catalog.svv_columns sc
    on sti.schema = sc.table_schema
   and sti.table = sc.table_name
   and sti.database = sc.table_catalog
 where sti.table = 'scm_cmmn_cd'
   and sc.ordinal_position > 0
   and sc.ordinal_position < 5
group by sti.table
;
