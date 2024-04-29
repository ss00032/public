CREATE OR REPLACE FUNCTION FN_TABLE_KEY_CHK(table_name VARCHAR)
RETURNS BOOLEAN AS $$
DECLARE
  pk_position INT;
  pk_columns VARCHAR(256);
  column_name VARCHAR;
  query_str VARCHAR;
  result_count INT;
BEGIN
  -- 테이블의 Primary Key 컬럼의 순서를 가져옴
  SELECT ordinal_position INTO pk_position
  FROM information_schema.columns
  WHERE table_schema = 'dept_dw'
    AND table_name = table_name
    AND column_name = 'reg_dtm';

  -- pk_position보다 작은 컬럼 목록을 가져와서 결과값들을 ,로 구분하여 나열
  FOR column_name IN
    SELECT isc.column_name
    FROM information_schema.columns isc
    JOIN pg_catalog.pg_stat_all_tables pgt ON pgt.schemaname = isc.table_schema
      AND pgt.relname = isc.table_name
    JOIN pg_catalog.pg_attribute pga ON pgt.relid = pga.attrelid
      AND isc.column_name = pga.attname
    WHERE isc.table_name = table_name
      AND pga.attnum < pk_position
  LOOP
    pk_columns := pk_columns || column_name || ',';
  END LOOP;

  -- 마지막 쉼표 제거
  pk_columns := LEFT(pk_columns, LENGTH(pk_columns) - 1);

  -- pk 중복 체크를 위한 쿼리 생성
  query_str := 'SELECT COUNT(1) FROM (SELECT ' || pk_columns ||
               ' FROM ' || table_name || ' GROUP BY ' || pk_columns ||
               ' HAVING COUNT(1) > 1) sub';

  -- 쿼리 실행
  EXECUTE query_str INTO result_count;

  IF result_count = 0 THEN
     RETURN TRUE;
  ELSE
     RETURN FALSE;
  END IF;
END;
$$ LANGUAGE plpgsql STABLE;
