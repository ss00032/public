CREATE OR REPLACE procedure SP_TABLE_KEY_CHK_2(tblnm VARCHAR, result_value OUT INTEGER )
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
  counter INT := 1;
  rec RECORD;
BEGIN
  v_table_schema := 'dept_dw';
  -- ���̺��� Primary Key �÷��� ������ ������
  select ordinal_position into pk_position
    from information_schema.columns
   where columns.table_schema = 'dept_dw'
     and columns.table_name = 'scm_cmmn_cd'
     and columns.column_name = 'reg_dtm';
  pk_columns := '';
  ordpos := 0;
  RAISE INFO '???? : %', tblnm;
  -- pk_position���� ���� �÷� ����� �����ͼ� ��������� ,�� �����Ͽ� ����
  while counter < pk_position loop
    SELECT pga.attnum INTO ordinal_position
      FROM information_schema.columns isc
      JOIN pg_catalog.pg_stat_all_tables pgt 
        ON pgt.schemaname = isc.table_schema
       AND pgt.relname = isc.table_name
      JOIN pg_catalog.pg_attribute pga ON pgt.relid = pga.attrelid
       AND isc.column_name = pga.attname
     WHERE isc.table_name = v_table_name
       AND pga.attnum = counter
   raise INFO '???? : %', counter;
   counter := counter + 1;
   ordpos := ordpos + ordinal_position;
  END LOOP;
  -- ������ ��ǥ ����
  --pk_columns := LEFT(pk_columns, LENGTH(pk_columns) - 1);
  -- pk �ߺ� üũ�� ���� ���� ����
  query_str := 'SELECT COUNT(1) FROM (SELECT str_cd,shop_cd,md_cd FROM dept_dw.smt_shop_md_m GROUP BY str_cd,shop_cd,md_cd HAVING COUNT(1) > 1)';
  --query_str := 'SELECT COUNT(1) FROM (SELECT ' || pk_columns || ' FROM ' || table_schema || '.' || table_name || ' GROUP BY ' || pk_columns || ' HAVING COUNT(1) > 1)';
  -- ���� ����
  EXECUTE query_str INTO result_count;
  IF result_count = 0 THEN
     result_value := ordpos;
  ELSE
     result_value := ordpos;
  END IF;
END;
$$
;