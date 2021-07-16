REM gfc_compress.sql
rem (c) Go-Faster Consultancy Ltd. www.go-faster.co.uk (c)2021
set serveroutput on echo on termout on
clear screen
spool gfc_compress

-------------------------------------------------------------------------------------------------------
-- gfc_compress package header
-------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE SYSADM.gfc_compress AS 
PROCEDURE main(/*p_owner                     VARCHAR2 DEFAULT 'SYSADM'
              ,*/p_table_name                VARCHAR2 DEFAULT ''
              ,p_partition_name            VARCHAR2 DEFAULT ''
              ,p_compression               VARCHAR2 DEFAULT ''
              ,p_compression_threshold_pct INTEGER  DEFAULT 95
              ,p_max_segs                  INTEGER  DEFAULT NULL
              ,p_max_secs                  INTEGER  DEFAULT 3600
              ,p_min_blks                  INTEGER  DEFAULT 128
              ,p_sample_pct                NUMBER   DEFAULT .1
              ,p_gather_stats              BOOLEAN  DEFAULT TRUE
              ,p_test_mode                 BOOLEAN  DEFAULT FALSE
              ,p_debug_level               INTEGER  DEFAULT 5
              );
END gfc_compress;
/
show errors 

-------------------------------------------------------------------------------------------------------
-- gfc_compress package body
-------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE BODY SYSADM.gfc_compress AS 
-------------------------------------------------------------------------------------------------------
k_module          CONSTANT VARCHAR2(64) := $$PLSQL_UNIT;
k_nls_date_format CONSTANT VARCHAR2(20) := 'hh24:mi:ss dd.mm.yy:';

l_debug_level  INTEGER := 0;  -- debug level of package
l_debug_indent INTEGER := 0;  -- indentation level of debug message
-------------------------------------------------------------------------------------------------------
-- print boolean value
-------------------------------------------------------------------------------------------------------
FUNCTION show_bool(p_bool BOOLEAN) RETURN VARCHAR IS
BEGIN
  IF p_bool THEN
    RETURN 'TRUE';
  ELSE
    RETURN 'FALSE';
  END IF;
END show_bool;
-------------------------------------------------------------------------------------------------------
-- to optionally print debug text during package run time
-------------------------------------------------------------------------------------------------------
PROCEDURE debug_msg(p_text VARCHAR2 DEFAULT ''
                   ,p_debug_level INTEGER DEFAULT 5) IS
  l_time VARCHAR2(30);
BEGIN
  l_time := TO_CHAR(SYSDATE,k_nls_date_format);
  IF p_debug_level <= l_debug_level AND p_text IS NOT NULL THEN
    sys.dbms_output.put_line(l_time||LPAD('.',l_debug_indent,'.')||'('||p_debug_level||')'||p_text);
  END IF;
END debug_msg;
-------------------------------------------------------------------------------------------------------
-- to permit debug when setting action
-------------------------------------------------------------------------------------------------------
PROCEDURE set_action(p_action_name VARCHAR2 DEFAULT ''
                    ,p_debug_level INTEGER DEFAULT 5) IS
BEGIN
  l_debug_indent := l_debug_indent + 1;
  dbms_application_info.set_action(action_name=>p_action_name);
  debug_msg(p_text=>'Setting action to: '||p_action_name,p_debug_level=>p_debug_level);
END set_action;	        
-------------------------------------------------------------------------------------------------------
-- to permit debug code when unseting action
-------------------------------------------------------------------------------------------------------
PROCEDURE unset_action(p_action_name VARCHAR2 DEFAULT ''
                      ,p_debug_level INTEGER DEFAULT 7) IS
BEGIN
  IF l_debug_indent > 0 THEN
    l_debug_indent := l_debug_indent - 1;
  END IF;
  dbms_application_info.set_action(action_name=>p_action_name);
  debug_msg(p_text=>'Resetting action to: '||p_action_name,p_debug_level=>p_debug_level);                
END unset_action;
-------------------------------------------------------------------------------------------------------
-- fts_secs() - calculate seconds between two timestamps
-------------------------------------------------------------------------------------------------------
FUNCTION fts_secs(t0 timestamp, t1 timestamp) RETURN number IS 
BEGIN
  RETURN round(extract(second from (t1-t0)),3)
       + 60*extract(minute from (t1-t0))
       + 3600*extract(hour from (t1-t0))
       + 86400*extract( day from (t1-t0));
END fts_secs;
-------------------------------------------------------------------------------------------------------
-- ts_secs - print seconds between two 
-------------------------------------------------------------------------------------------------------
PROCEDURE ts_secs(p_text VARCHAR2 DEFAULT ''
                 ,t0 timestamp
				 ,t1 timestamp) IS 
  l_secs NUMBER;
BEGIN
  l_secs := fts_secs(t0,t1);
  debug_msg(LTRIM(p_text||' ['||l_secs||' secs]'));
END ts_secs;
-------------------------------------------------------------------------------------------------------
PROCEDURE main(/*p_owner                     VARCHAR2 DEFAULT 'SYSADM'
              ,*/p_table_name                VARCHAR2 DEFAULT ''
              ,p_partition_name            VARCHAR2 DEFAULT ''
              ,p_compression               VARCHAR2 DEFAULT ''              
              ,p_compression_threshold_pct INTEGER  DEFAULT 95
              ,p_max_segs                  INTEGER  DEFAULT NULL
              ,p_max_secs                  INTEGER  DEFAULT 3600
              ,p_min_blks                  INTEGER  DEFAULT 128
              ,p_sample_pct                NUMBER   DEFAULT .1
              ,p_gather_stats              BOOLEAN  DEFAULT TRUE
              ,p_test_mode                 BOOLEAN  DEFAULT FALSE
              ,p_debug_level               INTEGER  DEFAULT 5
              ) IS 

  l_sql CLOB;
  l_compression_type INTEGER; /*dbms_compression types*/
  l_compression_pct  NUMBER;  /*percentage of rows at this compression type*/
  l_num_rows         INTEGER; /*number of rows at compression type*/
  l_tot_rows         INTEGER; /*total number of sampled rows*/
  l_row_count        INTEGER   := 0; /*row counter*/
  l_t0 CONSTANT      TIMESTAMP := SYSTIMESTAMP;
  l_t1               TIMESTAMP;
  l_t2               TIMESTAMP;
  l_module           VARCHAR2(64);
  l_action           VARCHAR2(64);
  l_object           VARCHAR2(400);
  
BEGIN
  dbms_application_info.read_module(module_name=>l_module, action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module, action_name=>'main()');
  l_debug_level := p_debug_level;

  debug_msg(k_module||'.MAIN',6);
  /*debug_msg('owner='||p_owner,6);*/
  debug_msg('table_name='||p_table_name,6);
  debug_msg('partition_name='||p_partition_name,6);
  debug_msg('compression_threshold='||p_compression_threshold_pct||'%',6);
  debug_msg('max_secs='||p_max_secs,6);
  debug_msg('max_segs='||p_max_segs,6);
  debug_msg('min_blks='||p_min_blks,6);
  debug_msg('sample='||p_sample_pct||'%',6);
  debug_msg('gather_stats='||show_bool(p_gather_stats),6);
  debug_msg('test mode='||show_bool(p_test_mode),6);
  
  FOR i IN (
    with p as (
    select /*table_owner,*/ table_name
    ,      'PARTITION' segment_type
    ,      partition_name
    ,      compression, compress_for, num_rows, blocks, avg_row_len
    ,      last_analyzed
    from   user_tab_partitions p
    where  subpartition_count = 0 /*omit subpartitioned partitions*/
    UNION ALL
    select /*table_owner,*/ table_name
    ,      'SUBPARTITION' segment_type
    ,      subpartition_name
    ,      compression, compress_for, num_rows, blocks, avg_row_len
    ,      last_analyzed
    from   user_tab_subpartitions s
    ), x as (
    select p.* 
    ,      p.num_Rows/NULLIF(LEAST(p.blocks,s.blocks),0)*p.avg_Row_len/NVL(s.bytes/s.blocks,8192) comp_factor
    from p
      left outer join user_segments s
      on /*s.owner = p.table_owner
      and*/ s.segment_name = p.table_name
      and s.partition_name = p.partition_name
      and s.segment_type = 'TABLE '||p.segment_type
    where  /*(p.table_owner = p_owner 
           OR p_owner IS NULL) 
    and    */(p.table_name like p_table_name 
           OR regexp_like(p.table_name,p_table_name)
           OR p_table_name IS NULL)
    and    (p.partition_name like p_partition_name 
           OR regexp_like(p.partition_name,p_partition_name)
           OR p_partition_name IS NULL)
    and    (  p.compression = p_compression
           OR p.compress_for = p_compression
           OR p_compression IS NULL)
    and    p.num_rows > 0 /*must contain rows*/
    and    p.blocks >= p_min_blks /*at least 128 blocks*/
--  and    p.compression != 'DISABLED' /*omit partitions where compression not specified*/
--  and    not p.table_name like 'PS%MV'
    and    not p.table_name like 'PSTREESELECT%' /*omit tree selectors*/
    --and (rownum <= p_max_segs OR p_max_segs <= 0 OR p_max_segs IS NULL)
    )
    select x.* 
    from   x
    where  (x.compression = 'ENABLED' AND x.comp_factor < 5)
    or     (x.compression = 'DISABLED' AND x.comp_factor > 2)
    order by comp_factor
  ) LOOP
    l_row_count := l_row_count + 1;
	IF p_max_segs IS NULL OR p_max_segs = 0 THEN
      NULL;
    ELSIF l_row_count > p_max_segs THEN   
      debug_msg('Max segments ('||p_max_segs||') exceeded');
      EXIT;
	END IF;

    IF    i.compression  = 'DISABLED'       THEN l_compression_type := dbms_compression.comp_nocompress /*1*/;
	ELSIF i.compression  = 'ENABLED'        THEN 
      IF   i.compress_for = 'BASIC'         THEN l_compression_type := dbms_compression.comp_basic /*4096*/;
      ELSIF i.compress_for = 'QUERY LOW'    THEN l_compression_type := dbms_compression.comp_query_low /*8*/;
      ELSIF i.compress_for = 'QUERY HIGH'   THEN l_compression_type := dbms_compression.comp_query_high /*16*/;
      ELSIF i.compress_for = 'ARCHIVE LOW'  THEN l_compression_type := dbms_compression.comp_archive_low /*32*/;
      ELSIF i.compress_for = 'ARCHIVE HIGH' THEN l_compression_type := dbms_compression.comp_archive_high /*64*/;
      ELSE raise_application_error(20001,'Unknown compression level');
      END IF;
    END IF;
    
    l_object := /*i.table_owner||'.'||*/i.table_name||'.'||i.partition_name;
    debug_msg(l_row_count||':'||l_object||':'||i.num_rows||' rows, '||i.blocks||' blocks, compress for '||NVL(i.compress_for,i.compression)||', factor:'||round(i.comp_factor,3),4);

    l_sql := 'with x as (select rowid, DBMS_COMPRESSION.get_compression_type ('''||user||''', '''||i.table_name||''', rowid, '''||i.partition_name||''') compression_type
from '||user||'.'||i.table_name||' '||i.segment_type||' ('||i.partition_name||') sample ('||p_sample_pct||',1)
), y as (select compression_type, count(*) num_rows from  x group by compression_type
), z as (select y.*, 100*ratio_to_report(num_rows) over () pct, sum(num_rows) over () tot_rows from y
)
select pct, num_rows, tot_rows from z where compression_type = :1';
    BEGIN
      dbms_application_info.set_module(module_name=>k_module, action_name=>SUBSTR('Analyze '||l_object,1,64));
      debug_msg(l_sql,7);
      l_t1 := SYSTIMESTAMP;
      EXECUTE IMMEDIATE l_sql INTO l_compression_pct, l_num_rows, l_tot_rows USING l_compression_type;
    EXCEPTION 
      WHEN no_data_found 
        THEN 
		  l_compression_pct := 0;
		  l_num_rows := 0;
          l_tot_rows := 0;
    END;
    l_t2 := SYSTIMESTAMP;
    ts_secs('Analyze',l_t1,l_t2);

    --take action if necessary
    IF l_compression_pct >= p_compression_threshold_pct THEN 
      debug_msg('Compress for '||NVL(i.compress_for,i.compression)||': ('||l_num_rows||'/'||l_tot_rows||') '||round(l_compression_pct,2)||'% >= threshold '||p_compression_threshold_pct||'% - No need to rebuild',6);
      l_row_count := l_row_count - 1;
    ELSE
      debug_msg('Compress for '||NVL(i.compress_for,i.compression)||': ('||l_num_rows||'/'||l_tot_rows||') '||round(l_compression_pct,2)||'% < threshold '||p_compression_threshold_pct||'%',6);
      l_sql := 'ALTER TABLE '||user||'.'||i.table_name||' MOVE '||i.segment_type||' '||i.partition_name||' ONLINE';
      dbms_application_info.set_module(module_name=>k_module, action_name=>SUBSTR('Rebuild '||l_object,1,64));
      l_t1 := SYSTIMESTAMP;
      IF p_test_mode THEN
        debug_msg('Test Mode:'||l_sql);
      ELSE
        debug_msg(l_sql);
        EXECUTE IMMEDIATE l_sql;
      END IF;
      l_t2 := SYSTIMESTAMP;
      ts_secs('Move',l_t1,l_t2);
    
      IF p_gather_stats AND NOT p_test_mode THEN
        dbms_application_info.set_module(module_name=>k_module, action_name=>SUBSTR('Stats '||l_object,1,64));
        l_t1 := SYSTIMESTAMP;
        dbms_stats.gather_table_stats(ownname=>user, tabname=>i.table_name, partname=>i.partition_name, force=>TRUE);
        l_t2 := SYSTIMESTAMP;
        ts_secs('Stats',l_t1,l_t2);
      END IF;
    END IF;
    
    IF fts_secs(l_t0,l_t2) > p_max_secs THEN 
      debug_msg('Max time ('||p_max_secs||' secs) exceeded');
      exit;
    END IF;
  END LOOP;
  dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
EXCEPTION
  WHEN OTHERS THEN
    dbms_application_info.set_module(module_name=>l_module, action_name=>l_action);
    RAISE;
END main;
END gfc_compress;
/
show errors


desc sysadm.gfc_compress
spool off

