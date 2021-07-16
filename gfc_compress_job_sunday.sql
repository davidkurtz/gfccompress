REM gfc_compress_job.sql
clear screen
set termout on trimspool on lines 200 pages 999 pause off
spool gfc_compress_job
ALTER SESSION SET time_zone = 'US/Eastern';
ALTER SESSION SET current_schema = SYSADM;
----------------------------------------------------------------------------------------------------
--schedule job to run every Sunday at 8am for 8 hours
----------------------------------------------------------------------------------------------------
--exec dbms_scheduler.stop_job(job_name => 'SYSADM.GFC_COMPRESS_SUNDAY');
exec dbms_scheduler.drop_job(job_name => 'SYSADM.GFC_COMPRESS_SUNDAY');
begin
  dbms_scheduler.create_job(  
    job_name      =>  'GFC_COMPRESS_SUNDAY',  
    job_type      =>  'PLSQL_BLOCK',  
    job_action    =>  'begin sysadm.gfc_partdata.comp_attrib; sysadm.gfc_compress.main(p_table_name=>''PS%'', p_partition_name=>''_20[[:digit:]]{2}'', p_max_secs=>36000, p_debug_level=>5); end;',  
    start_date    =>  sysdate,
    repeat_interval => 'freq=weekly; byday=SUN; byhour=6',
    end_date      =>  null,
    enabled       =>  TRUE,  
    comments      =>  'GFC_COMPRESS Sunday 6am 10-hour job');
end;
/
----------------------------------------------------------------------------------------------------
spool off
@@gfc_compress_job_report.sql