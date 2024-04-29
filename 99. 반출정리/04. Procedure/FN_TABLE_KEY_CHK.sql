CREATE OR REPLACE FUNCTION FN_TABLE_KEY_CHK(table_name VARCHAR)
RETURNS BOOLEAN AS $$
DECLARE
  pk_position INT;
  pk_columns VARCHAR(256);
  column_name VARCHAR;
  query_str VARCHAR;
  result_count INT;
BEGIN
  -- ���̺��� Primary Key �÷��� ������ ������
  SELECT ordinal_position INTO pk_position
  FROM information_schema.columns
  WHERE table_schema = 'dept_dw'
    AND table_name = table_name
    AND column_name = 'reg_dtm';

  -- pk_position���� ���� �÷� ����� �����ͼ� ��������� ,�� �����Ͽ� ����
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

  -- ������ ��ǥ ����
  pk_columns := LEFT(pk_columns, LENGTH(pk_columns) - 1);

  -- pk �ߺ� üũ�� ���� ���� ����
  query_str := 'SELECT COUNT(1) FROM (SELECT ' || pk_columns ||
               ' FROM ' || table_name || ' GROUP BY ' || pk_columns ||
               ' HAVING COUNT(1) > 1) sub';

  -- ���� ����
  EXECUTE query_str INTO result_count;

  IF result_count = 0 THEN
     RETURN TRUE;
  ELSE
     RETURN FALSE;
  END IF;
END;
$$ LANGUAGE plpgsql STABLE;
