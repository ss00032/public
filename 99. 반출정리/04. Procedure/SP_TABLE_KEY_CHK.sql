CREATE OR REPLACE procedure SP_TABLE_KEY_CHK(v_table_name VARCHAR, v_from_dt VARCHAR, v_to_dt VARCHAR, result_value OUT INTEGER )
    LANGUAGE plpgsql
AS $$
DECLARE
  pk_position INT;
  pk_columns VARCHAR(4000);
  v_table_schema VARCHAR;
  v_column_name VARCHAR;
  query_str VARCHAR(6000);
  result_count INT;
  ordpos INT;
  counter INT;
  rec RECORD;
  v_juje_name VARCHAR;
  base_yn INT;
  v_from_ym VARCHAR;
  v_to_ym VARCHAR;
BEGIN
  v_table_schema := 'ansor';
  v_column_name := 'etl_wrk_dtm';
  v_juje_name := substring(v_table_name,1,1);
  v_from_ym := substring(v_from_dt,1,6);
  v_to_ym := substring(v_to_dt,1,6);

  IF v_juje_name = 'a' THEN
      v_table_schema := 'anana';
  ELSIF v_juje_name = 'r' THEN
      v_table_schema := 'anrep';
  ELSE
      v_table_schema := 'ansor';
  END IF;

  -- 테이블의 Primary Key 컬럼의 순서를 가져옴
  SELECT ordinal_position into pk_position
    FROM information_schema.columns
   WHERE columns.table_schema = v_table_schema
     AND columns.table_name = v_table_name
     AND columns.column_name = v_column_name;
    
  --기준일자 또는 기준년월이 존재하는 테이블
  SELECT case when isc.column_name = 'cri_ym' then 1 when isc.column_name = 'cri_ymd' then 2 else 0 end into base_yn
    FROM information_schema.columns isc
    JOIN pg_catalog.pg_stat_all_tables pgt 
      ON pgt.schemaname = isc.table_schema
     AND pgt.relname = isc.table_name
    JOIN pg_catalog.pg_attribute pga
      ON pgt.relid = pga.attrelid
     AND isc.column_name = pga.attname
   WHERE isc.table_schema = v_table_schema
     AND isc.table_name = v_table_name
     AND pga.attnum < 4
     AND (column_name = 'cri_ym' or column_name = 'cri_ymd')
   ORDER BY pga.attnum asc;
  
  pk_columns := '';
  -- pk_position보다 작은 컬럼 목록을 가져와서 결과값들을 ,로 구분하여 나열
  FOR rec IN
    SELECT isc.column_name as col_name
         , pga.attnum as ordinal_position
      FROM information_schema.columns isc
      JOIN pg_catalog.pg_stat_all_tables pgt 
        ON pgt.schemaname = isc.table_schema
       AND pgt.relname = isc.table_name
      JOIN pg_catalog.pg_attribute pga 
        ON pgt.relid = pga.attrelid
       AND isc.column_name = pga.attname
     WHERE isc.table_schema = v_table_schema
       AND isc.table_name = v_table_name
       AND pga.attnum < pk_position
     ORDER BY pga.attnum asc
  loop
    -- pk_columns :=  ',' || pk_columns || rec.col_name;
    IF rec.ordinal_position = 1 THEN
      pk_columns := rec.col_name;
    ELSE
      pk_columns := pk_columns || ',' || rec.col_name;
    END IF;
  END LOOP;

  -- pk 중복 체크를 위한 쿼리 생성
  IF base_yn = 1 THEN
    query_str := 'SELECT COUNT(1) FROM (SELECT ' || pk_columns || ' FROM ' || v_table_schema || '.' || v_table_name || ' WHERE cri_ym BETWEEN ' || v_from_ym || ' AND ' || v_to_ym || ' GROUP BY ' || pk_columns || ' HAVING COUNT(1) > 1)';
  ELSIF base_yn = 2 THEN
    query_str := 'SELECT COUNT(1) FROM (SELECT ' || pk_columns || ' FROM ' || v_table_schema || '.' || v_table_name || ' WHERE cri_ymd BETWEEN ' || v_from_dt || ' AND ' || v_to_dt || ' GROUP BY ' || pk_columns || ' HAVING COUNT(1) > 1)';
  ELSE
    query_str := 'SELECT COUNT(1) FROM (SELECT ' || pk_columns || ' FROM ' || v_table_schema || '.' || v_table_name || ' GROUP BY ' || pk_columns || ' HAVING COUNT(1) > 1)';
  END IF;

  -- 쿼리 실행
  EXECUTE query_str INTO result_count;
  IF result_count = 0 THEN
     result_value := 0;
  ELSE
     result_value := 1;
  END IF;
END;
$$;