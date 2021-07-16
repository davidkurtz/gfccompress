REM gfc_compress_job_report.sql
clear screen
set termout on trimspool on lines 200 pages 999 pause off
spool gfc_compress_job_report
----------------------------------------------------------------------------------------------------
--report on scheduled job
----------------------------------------------------------------------------------------------------
column owner format a8
column job_name format a20
column job_creator format a8
column job_subname format a15
column job_class format a20
column user_name format a20
column client_id format a32
column repeat_interval format a40
column flags format 9999999999999999
column running_instance heading 'Running|Instance' format 999
column resource_consumer_group heading 'Resource|Consumer|Group' format a15
column slave_process_id heading 'Slave|Process|ID' format 999999
column session_id format a10
column output format a160 wrap on
column event_queue_owner format a8 heading 'Event|Queue|Name'
column event_queue_name format a20
column event_queue_agent format a20
column event_condition format a20
column event_rule format a20
column schedule_owner format a8 heading 'Schedule|Name'
column schedule_name format a20
column program_owner format a8 heading 'Program|Name'
column program_name format a20
column connect_credential_owner format a8 heading 'Connect|Credential|Owner'
column connect_credential_name format a20
column credential_owner format a8 heading 'Credential|Owner'
column credential_name format a20 
column destination_owner format a8 heading 'Dest|Owner'
column destination format a20 
column file_watcher_owner format a8 heading 'File|Watcher|Owner'
column file_watcher_name format a20 
column ITYP_OWNER heading 'Ityp|Owner'
column ityp_name format a20
column nls_env format a60 wrap on
column raise_events format a20
column source format a30
column next_run_date format a40
----------------------------------------------------------------------------------------------------
ttitle 'all_scheduler_running_jobs'
select * from all_scheduler_running_jobs
where job_name LIKE 'GFC_COMPRESS%'
/
----------------------------------------------------------------------------------------------------
ttitle 'all_scheduler_jobs'
select * from all_scheduler_jobs
where job_name LIKE 'GFC_COMPRESS%'
order by start_date desc 
/
----------------------------------------------------------------------------------------------------
ttitle 'all_scheduler_job_log'
select * from all_scheduler_job_log
where job_name LIKE 'GFC_COMPRESS%'
and log_date > sysdate-7
order by log_date desc 
/
----------------------------------------------------------------------------------------------------
ttitle 'all_scheduler_job_run_details'
select log_id, log_date, owner, job_name, status, error#
, req_start_date, actual_start_date, run_duration, instance_id, session_id, slave_pid, cpu_used
--, credential_owner, credential_name, destination_owner, destination, additional_info
, errors, output
from all_scheduler_job_run_details
where job_name LIKE 'GFC_COMPRESS%'
and log_date > sysdate-7
order by log_date desc
/
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
ttitle 'Recent job ASH'
column avg_para format 99.9
with x as (
select cast(min(sample_time) as date) min_sample_time, cast(max(sample_time) as date) max_sample_time, 
NVL(qc_session_id,session_id) session_id,
action, sum(1) ash_secs, count(distinct CAST(sample_time AS date)) elap_secs
from gv$active_session_History
where module like 'GFC_COMPRESS%'
and not action like 'Analyze%'
group by action, NVL(qc_session_id,session_id) 
--, trunc(sample_time,'mi')
) select x.*, round(ash_secs/nullif(elap_secs,0),2) avg_para 
from x
--where action like '%LEDGER_2019_0__T_USCORE'
order by max_sample_time desc
/
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
ttitle 'Historical job ASH'
with x as (
select cast(min(sample_time) as date) min_sample_time, cast(max(sample_time) as date) max_sample_time, 
NVL(qc_session_id,session_id) session_id,
action, sum(10) ash_secs, 10*count(distinct CAST(sample_time AS date)) elap_secs
from dba_hist_active_Sess_history
where module like 'GFC_COMPRESS%'
and not action like 'Analyze%'
and sample_time > sysdate-7
group by action, NVL(qc_session_id,session_id) 
--, trunc(sample_time,'mi')
) select x.*, round(ash_secs/nullif(elap_secs,0),2) avg_para 
from x
order by max_sample_time desc
/
----------------------------------------------------------------------------------------------------
ttitle off
spool off