REM gfc_compress_job_prtonly.sql
clear screen
set termout on trimspool on lines 200 pages 999 pause off
spool gfc_compress_job_prtonly
ALTER SESSION SET time_zone = 'US/Eastern';
ALTER SESSION SET current_schema = SYSADM;
----------------------------------------------------------------------------------------------------
--Midweek job for PRT only
----------------------------------------------------------------------------------------------------
exec dbms_scheduler.stop_job(job_name => 'SYSADM.GFC_COMPRESS_WEEKDAY');
exec dbms_scheduler.drop_job(job_name => 'SYSADM.GFC_COMPRESS_WEEKDAY');
/*
begin
  dbms_scheduler.create_job(  
    job_name      =>  'GFC_COMPRESS_WEEKDAY',  
    job_type      =>  'PLSQL_BLOCK',  
    job_action    =>  'begin sysadm.gfc_compress.main(p_table_name=>''PS%'', p_partition_name=>''_20[[:digit:]]{2}'', p_max_secs=>32000, p_debug_level=>5); end;',  
    start_date    =>  sysdate,
    repeat_interval => 'freq=weekly; byday=MON,TUE,WED,THU,FRI; byhour=6',
    end_date      =>  null,
    enabled       =>  TRUE,  
    comments      =>  'GFC_COMPRESS Weekday 6am 9-hour job - PRT only');
end;
*/
----------------------------------------------------------------------------------------------------
spool off
@@gfc_compress_job_report.sql