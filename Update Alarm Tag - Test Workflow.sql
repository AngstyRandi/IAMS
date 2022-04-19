/* Alarm Tagging SQL Script */
/* This script is to be triggered by a SQL Agent every 1 min interval */

/* Equivalent tables for reference */
/*
alarmList --> TestValues_Alarm
eventList --> TestValues_Master
Output_AlarmTagged --> Test_AlarmTagged
Output_TagComplete --> Test_Final
*/

/* On time matching */
/*
for triggering the SQL agent, use current system time
for windowing source data into a smaller time period, use the time the data is recorded into SQL table - the windowed time should then be in a small enough window with SCS_TIME
for performing any analytics, use SCS_TIME to reflect the real time the event occurs
for deleting and transferring data, use the time the data is recorded into SQL table
*/

/* Define variables (time periods) to filter rows by */
/* In a live production environment, use DATETIME_LOADED instead */
/* this is to window the time frame of data to keep the working data in-memory manageable */
/*
A minimum buffer window of 1 min using DATETIME_LOADED is provided to ensure that all relevant event pairs 
from alarmList and eventList has streamed in in respected to SCS_TIME (DATEANDTIME)

Buffer window: N-0min to N-1min
Working window: N-1min to N-2min
Excluded window: exceeding N-2min --> data should have been processed by then, so it can be transferred/deleted
*/
Declare @LastestTime datetime, @OneMinAgo datetime, @TwoMinAgo datetime, @CurrentTime datetime
select @LastestTime = max(TestValues_Master.DATEANDTIME), 
	   @OneMinAgo = dateadd(minute, -1, @LastestTime), 
	   @TwoMinAgo =  dateadd(minute, -2, @LastestTime),
	   @CurrentTime = getdate()
from TestValues_Master;

/* apply time filter and update relevant rows */
/* filters (where filter) for values t-1min to t-2min using DATETIME_LOADED */
/* In this case, since the data comes in a batch process, the DATEANDTIME field is used in lieu of it */
/* For a production environment, substitute DATEANDTIME where filter with DATETIME_LOADED; and the join on DATEANDTIME with SCS_TIME */
/* System time cannot be used as a filter as the logs will have higher latency on recording exceeding 1min during peak hour */
/* Hence, system time can only be used to trigger the agent at periodic intervals */
update TestValues_Master
set TestValues_Master.TnF = TestValues_Alarm.TnF
from TestValues_Master
inner join TestValues_Alarm
on TestValues_Master.eventID = TestValues_Alarm.eventID and TestValues_Master.DATEANDTIME = TestValues_Alarm.DATEANDTIME
where TestValues_Master.DATEANDTIME <= @OneMinAgo and TestValues_Master.DATEANDTIME >= @TwoMinAgo;

/* Transfer records from one table to another */
/* A temporary table is used so that the latest datetime which the data is loaded into the table is recorded */
/* One may remove the N-2min window limit in the transfer of files from 1 table to another as a sure check for any lagging rows of data*/
SELECT * INTO TempTable_ALTag
FROM TestValues_Master
where TestValues_Master.DATEANDTIME <= @OneMinAgo and TestValues_Master.DATEANDTIME >= @TwoMinAgo
Update TempTable_ALTag set DATETIME_LOADED = getdate();

/* Complete file transfer */
INSERT INTO Test_AlarmTagged
SELECT *
FROM TempTable_ALTag
DROP TABLE TempTable_ALTag;

/* Old records are removed */
/* This is to keep the indexing cost low via keeping the intermediate table size low too */
/* One may remove the N-2min window limit in the transfer of files from 1 table to another as a sure check for any lagging rows of data*/
DELETE FROM TestValues_Master
WHERE TestValues_Master.DATEANDTIME <= @OneMinAgo and TestValues_Master.DATEANDTIME >= @TwoMinAgo; 

DELETE FROM TestValues_Alarm
WHERE TestValues_Alarm.DATEANDTIME <= @OneMinAgo and TestValues_Alarm.DATEANDTIME >= @TwoMinAgo; 
