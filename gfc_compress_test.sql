REM gfc_compress_test.sql
clear screen 
set pages 99 lines 200 long 4000 timi on trimspool on
spool gfc_compress_test
alter session set nls_date_Format = 'hh24:mi:ss dd.mm.yy';
ALTER SESSION SET time_zone = 'US/Eastern';
ALTER SESSION SET current_schema = SYSADM;
----------------------------------------------------------------------------------------------------
--test user and table
----------------------------------------------------------------------------------------------------
--connect / as sysdba 
--drop package sys.gfc_compress;
--connect SYSADM/tiger@oracle_pdb
/*----------------------------------------------------------------------------------------------------
drop table t purge;

create table t
(a)
compress 
--for query low
partition by range (a)
interval (1e5) 
(partition p1 values less than (1e5) compress
) 
as select 
--floor(sqrt(rownum)) 
round(rownum,-3)
from dual connect by level <= 1e6
/

insert into t select rownum from dual connect by level <= 1e6;
commit;

alter table t compress;
alter table t move partition p1 compress;
exec dbms_stats.gatheR_table_stats(user,'T');

with x as (
select DBMS_COMPRESSION.get_compression_type ('SYSADM', 'T', rowid, 'P1') compression_type
from SYSADM.T PARTITION (P1) sample (1)
)
select compression_type, count(*) 
from x
group by compression_type
/

column table_name format a10
column partition_name format a10
column comp_factor format 999.999
select table_name, partition_name, num_rows, blocks, compression, compress_for, num_Rows/NULLIF(blocks,0)*avg_Row_len/8192 comp_factor
from user_tab_partitions
where table_name = 'T'
order by 1,2
/

--alter table ps_ledger_budg compress for query low;
--alter table ps_ledger compress for query low;

----------------------------------------------------------------------------------------------------
--sysadm test
----------------------------------------------------------------------------------------------------
set timi on serveroutput on
spool gfc_compress_test app
clear screen
--exec gfc_compress.main(p_partition_name=>'%2017%', p_max_segs=>2, p_max_secs=>60, p_debug_level=>7, p_test_mode=>TRUE);
--exec sysadm.gfc_compress.main(p_table_name=>'PS_LEDGER', p_partition_name=>'_201[[:digit:]]{1}', p_max_segs=>3, p_max_secs=>60, p_test_mode=>TRUE, p_debug_level=>7);
--exec gfc_compress.main(p_partition_name=>'%2017%', p_max_segs=>5, p_max_secs=>60, p_debug_level=>7);
--exec gfc_compress.main(p_partition_name=>'%2017%', p_max_secs=>600, p_debug_level=>6);
--exec gfc_compress.main(p_partition_name=>'%2018%', p_max_secs=>600, p_debug_level=>6);
--exec gfc_compress.main(p_partition_name=>'%2019%', p_max_secs=>10800, p_debug_level=>6);
--exec sysadm.gfc_compress.main(p_table_name=>'PS_LEDGER_BUDG', p_partition_name=>'%2018%', p_max_segs=>5, p_max_secs=>600, p_test_mode=>FALSE, p_debug_level=>6);
spool off
----------------------------------------------------------------------------------------------------
--create job to run for an hour
----------------------------------------------------------------------------------------------------
exec dbms_scheduler.drop_job(job_name => 'GFC_COMPRESS_SUNDAY');
exec dbms_scheduler.drop_job(job_name => 'GFC_COMPRESS_WEEKDAY');
begin
  dbms_scheduler.create_job(  
    job_name      =>  'GFC_COMPRESS_JOB',  
    job_type      =>  'PLSQL_BLOCK',  
  --job_action    =>  'begin sysadm.gfc_partdata.comp_attrib; sysadm.gfc_compress.main(p_table_name=>''PS_LEDGER_BUDG'', p_partition_name=>''%201%'', p_max_secs=>3600, p_debug_level=>6); end;',  
  --job_action    =>  'begin sysadm.gfc_partdata.comp_attrib; sysadm.gfc_compress.main(p_table_name=>''PS_LEDGER%'', p_partition_name=>''_20[[:digit:]]{2}'', p_max_secs=>28000, p_gather_stats=>TRUE, p_debug_level=>6); end;',  
    job_action    =>  'begin sysadm.gfc_partdata.comp_attrib; sysadm.gfc_compress.main(p_table_name=>''PS_LEDGER'', p_partition_name=>''_202[[:digit:]]{1}'', p_max_secs=>60, p_test_mode=>TRUE, p_debug_level=>7); end;',  
    start_date    =>  sysdate,  
    enabled       =>  TRUE,  
    auto_drop     =>  TRUE,  
    comments      =>  'GFC_COMPRESS 1-hour job');
end;
/
----------------------------------------------------------------------------------------------------*/
--exec sys.dbms_scheduler.STOP_JOB(job_name=>'GFC_COMPRESS_JOB', force=>true);
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
@@gfc_compress_job_report
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--select * from user_tab_stat_prefs where table_name = 'PS_LEDGER';
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
ttitle 'Partitions'
column table_name format a18
column partition_name format a30
column compress_for format a15
column empty_blocks heading 'Empty|Blocks' format 999999
column seg_blocks heading 'Segment|Blocks' format 999999999
column comp_factor heading 'Compression|Factor' format 999.999
column pct_rows heading '% Rows' format 99.9
column cum_pct_rows heading 'Cum %|Rows' format 999.9
with p as (
select table_name, 'PARTITION' segment_type,  partition_name, num_rows, blocks, empty_blocks, avg_row_len, compression, compress_for, last_analyzed
from user_tab_partitions
where subpartition_count = 0
union all
select table_name, 'SUBPARTITION', subpartition_name, num_rows, blocks, empty_blocks, avg_row_len, compression, compress_for, last_analyzed
from user_tab_subpartitions
), x as (
select p.*, s.blocks seg_blocks
, p.num_Rows/NULLIF(LEAST(p.blocks,s.blocks),0)*p.avg_Row_len/NVL(s.bytes/s.blocks,8192) comp_factor
--, p.blocks/s.blocks*100 hwm
, 100*ratio_to_report(num_Rows) over (partition by table_name) pct_rows
from p
  left outer join user_segments s
  ON s.segment_name = p.table_name
  and s.partition_name = p.partition_name
  and s.segment_type = 'TABLE '||p.segment_type
where NVL(p.blocks,s.blocks)>128
and regexp_like(p.partition_name,'20[[:digit:]]{2}')
--and p.table_name like 'PS%'
)
select x.* 
, sum(pct_rows) over (partition by table_name order by comp_factor rows between unbounded preceding and current row) cum_pct_rows
from x
order by comp_Factor, last_analyzed desc, 1,2,3
/
----------------------------------------------------------------------------------------------------
--select * from user_indexes where table_name IN('PS_LEDGER','PS_LEDGER_BUDG');
----------------------------------------------------------------------------------------------------
ttitle off
spool off
