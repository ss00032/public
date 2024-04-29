CREATE OR REPLACE procedure SP_TABLE_KEY_CHK_3(v_table_name VARCHAR, result_value OUT INTEGER )
    LANGUAGE plpgsql
AS $$
DECLARE
  pk_position INT;
  pk_columns VARCHAR(256);
  v_table_schema VARCHAR;
  v_column_name VARCHAR;
  query_str VARCHAR;
  result_count INT;
  ordpos INT;
  counter INT;
  rec RECORD;
BEGIN
  v_table_schema := 'dept_dw';
  v_column_name := 'reg_dtm';
  -- 테이블의 Primary Key 컬럼의 순서를 가져옴
  SELECT ordinal_position into pk_position
  FROM information_schema.columns
  WHERE columns.table_schema = v_table_schema
    AND columns.table_name = v_table_name
    AND columns.column_name = v_column_name;
  pk_columns := '';
  -- pk_position보다 작은 컬럼 목록을 가져와서 결과값들을 ,로 구분하여 나열
  FOR rec IN
    SELECT isc.column_name as col_name
         , pga.attnum as ordinal_position
      FROM information_schema.columns isc
      JOIN pg_catalog.pg_stat_all_tables pgt 
        ON pgt.schemaname = isc.table_schema
       AND pgt.relname = isc.table_name
      JOIN pg_catalog.pg_attribute pga ON pgt.relid = pga.attrelid
       AND isc.column_name = pga.attname
     WHERE isc.table_name = v_table_name
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
  query_str := 'SELECT COUNT(1) FROM (SELECT ' || pk_columns || ' FROM ' || v_table_schema || '.' || v_table_name || ' GROUP BY ' || pk_columns || ' HAVING COUNT(1) > 1)';
 
  -- 쿼리 실행
  EXECUTE query_str INTO result_count;
  IF result_count = 0 THEN
     result_value := 0;
  ELSE
     result_value := 1;
  END IF;
END;
$$
;