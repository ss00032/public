CREATE OR REPLACE procedure SP_TABLE_KEY_CHK_1(table_name VARCHAR, result_value OUT VARCHAR )
    LANGUAGE plpgsql
AS $$
DECLARE
  pk_position INT;
  pk_columns VARCHAR(256);
  table_schema VARCHAR;
  column_name VARCHAR;
  query_str VARCHAR;
  result_count INT;
  rec RECORD;
BEGIN
  table_schema := 'dept_dw';
  -- ���̺��� Primary Key �÷��� ������ ������
  SELECT ordinal_position INTO pk_position
  FROM information_schema.columns
  WHERE table_schema = table_schema
    AND columns.table_name = table_name
    AND column_name = 'reg_dtm';
  pk_columns := '';
  -- pk_position���� ���� �÷� ����� �����ͼ� ��������� ,�� �����Ͽ� ����
  FOR rec IN
    SELECT isc.column_name as col_name
         , pga.attnum as ordinal_position
      FROM information_schema.columns isc
      JOIN pg_catalog.pg_stat_all_tables pgt 
        ON pgt.schemaname = isc.table_schema
       AND pgt.relname = isc.table_name
      JOIN pg_catalog.pg_attribute pga ON pgt.relid = pga.attrelid
       AND isc.column_name = pga.attname
     WHERE isc.table_name = table_name
       AND pga.attnum < pk_position
  LOOP
    IF rec.ordinal_position = 1 THEN
      pk_columns := pk_columns || rec.col_name;
    ELSE
      pk_columns :=  ',' || pk_columns || rec.col_name;
    END IF;
  END LOOP;
  -- ������ ��ǥ ����
  --pk_columns := LEFT(pk_columns, LENGTH(pk_columns) - 1);
  -- pk �ߺ� üũ�� ���� ���� ����
  query_str := 'SELECT COUNT(1) FROM (SELECT str_cd,shop_cd,md_cd FROM dept_dw.smt_shop_md_m GROUP BY str_cd,shop_cd,md_cd HAVING COUNT(1) > 1)';
  --query_str := 'SELECT COUNT(1) FROM (SELECT ' || pk_columns || ' FROM ' || table_schema || '.' || table_name || ' GROUP BY ' || pk_columns || ' HAVING COUNT(1) > 1)';
  -- ���� ����
  EXECUTE query_str INTO result_count;
  IF result_count = 0 THEN
     result_value := pk_columns;
  ELSE
     result_value := pk_columns;
  END IF;
END;
$$
;