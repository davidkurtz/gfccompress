REM gfc_compress_setup.sql
column table_name format a18
column min_partition_name format a30
column max_partition_name format a30
set serveroutput on pages 99 lines 200
spool gfc_compress_setup


DECLARE
  l_sql CLOB;
BEGIN
  FOR i IN(
with l as (
  select DISTINCT DECODE(r.sqltablename,' ','PS_'||r.recname,r.sqltablename) table_name, ledger_type
  from ps_led_tmplt_tbl t, psrecdefn r
  where t.recname = r.recname
  and r.rectype = 0
)
select t.table_name, m.container_name, t.compression, t.compress_for, l.ledger_type, k.column_position FY
  from user_tables t
    LEFT OUTER JOIN l ON t.table_name = l.table_name
    LEFT OUTER JOIN user_mviews m 
      ON m.container_name = t.table_name
    LEFT OUTER JOIN user_part_key_columns k
      ON k.name = t.table_name
      AND k.object_type = 'TABLE'
      AND k.column_name = 'FISCAL_YEAR'
  where t.partitioned = 'YES'
  and k.column_position IS NOT NULL
  and t.table_name != 'PS_LEDGER'
  and (t.compress_for != 'QUERY LOW' OR t.compress_for IS NULL)
  and (l.ledger_type IS NOT NULL
    or m.container_name IS NOT NULL)
  order by 1
  ) LOOP
    l_sql := 'ALTER TABLE '||i.table_name||' COMPRESS FOR QUERY LOW';
    dbms_output.put_line(l_sql);
    EXECUTE IMMEDIATE l_Sql;
  END LOOP;
END;
/
  
  
   

DECLARE
  l_sql CLOB;
  l_max_comp_part_pos INTEGER;
BEGIN
  SELECT MAX(partition_position)
  INTO   l_max_comp_part_pos
  FROM   user_tab_partitions
  WHERE  REGEXP_SUBSTR(partition_name,'20[[:digit:]]{2}_[[:digit:]]{2}') <= '2020_09'
  AND    table_name like 'PS_LEDGER';
  
  dbms_output.put_line('l_max_comp_part_pos='||l_max_comp_part_pos);
  
  FOR i IN(
    SELECT table_name, partition_name, compression, compress_for, num_rows, partition_position
    ,      CASE WHEN partition_position <= l_max_comp_part_pos AND compression = 'DISABLED' THEN 'COMPRESS FOR QUERY LOW' 
                WHEN partition_position >  l_max_comp_part_pos AND compression = 'ENABLED'  THEN 'NOCOMPRESS' 
           END cmd
    FROM   user_tab_partitions
    WHERE  REGEXP_LIKE(partition_name,'20[[:digit:]]{2}_(BF|CF|[[:digit:]]{2})')
    AND    table_name like 'PS_LEDGER'
  ) LOOP
    IF i.cmd IS NOT NULL THEN
    l_sql := 'ALTER TABLE '||i.table_name||' MODIFY PARTITION '||i.partition_name||' '||i.cmd;
      dbms_output.put_line(l_sql);
      EXECUTE IMMEDIATE l_Sql;
    END IF;
  END LOOP;

END;
/


SELECT table_name, compression, compress_for
, min(partition_name) min_partition_name
, max(partition_name) max_partition_name
, count(*)
FROM   user_tab_partitions
WHERE  REGEXP_LIKE(partition_name,'_20[[:digit:]]{2}')
group by table_name, compression, compress_for
order by 1,4
/


SELECT table_name, compression, compress_for
, min(partition_name) min_partition_name
, max(partition_name) max_partition_name
, count(*)
FROM   user_tab_partitions
WHERE  REGEXP_LIKE(partition_name,'(_2020_(09|10|11|12|CF)|_202[1-9]_)')
AND    table_name = 'PS_LEDGER'
group by table_name, compression, compress_for
order by 1,4
/



set serveroutput on pages 99 lines 200
--exec sysadm.gfc_compress.main(p_table_name=>'PS_LEDGER', p_partition_name=>'_202[[:digit:]]{1}', p_max_segs=>5, p_max_secs=>60, p_sample_pct=>.01, p_test_mode=>TRUE, p_debug_level=>6);
--exec sysadm.gfc_compress.main(p_table_name=>'PS_LEDGER', p_partition_name=>'_202[1-9]{1}', p_max_segs=>5, p_max_secs=>60, p_test_mode=>TRUE, p_debug_level=>6);
--exec sysadm.gfc_compress.main(p_table_name=>'PS_LEDGER', p_partition_name=>'(_2020_(09|10|11|12|CF)|_202[1-9]_)', p_compression=>'DISABLED', p_max_secs=>60, p_sample_pct=>.01, p_test_mode=>TRUE, p_debug_level=>6);
rollback;

spool off