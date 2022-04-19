/* Nuisance Event Tagging */

/* Nuisance Events are defined as both Repeat and Toggle Events */
/*
Repeat events:
- Same asset
- Event Description and Event Status of N-0 and N-1 repeats in a A-A pattern
- Time span between N-0 and N-1 is <= 1 min

Toggle events:
- Same asset
- Event Description and Event Status of (N-0 and N-2) and (N-1 and N-3) repeats in a A-B-A-B pattern
- Time span between N-0 and N-3 is <= 1 min
- A more stringent version goes with a triple repeat combo pattern (A-B-A-B-A-B)
*/

/* Key Parameters */
/*
target table: Test_AlarmTagged --> Output_AlarmTagged
output table: Test_Final --> Output_TagComplete
RepeatAlarm (test) = RepeatAlarm (production)
ToggleEventA (test) = AltAlarm2 (production)
ToggleEventB (test) = AltAlarm3 (production)
NuisanceAlarm (test) = NuisanceAlarm (production)
*/

/* Define variables (time periods) to filter rows by */
/* In a live production environment, use DATETIME_LOADED instead */
/* this is to window the time frame of data to keep the working data in-memory manageable */
/*
A minimum buffer window of 1 min using DATETIME_LOADED is provided to ensure that all relevant events from the alarm tagged logs
has streamed in in respected to SCS_TIME (DATEANDTIME)

Buffer window: N-0min to N-1min
Working window: N-1min to N-2min
Excluded window: exceeding N-2min --> data should have been processed by then, so it can be transferred/deleted
*/
Declare @LastestTime datetime, @OneMinAgo datetime, @TwoMinAgo datetime, @CurrentTime datetime
select @LastestTime = max(Test_AlarmTagged.DATEANDTIME), 
	   @OneMinAgo = dateadd(minute, -1, @LastestTime), 
	   @TwoMinAgo =  dateadd(minute, -2, @LastestTime),
	   @CurrentTime = getdate()
from Test_AlarmTagged;


/* Extract lag values for evaluation */
/* Note: dependencies must be execute as a separate query first before generating values reliant on those values */
select *,
	Lag(eventDesc, 1) OVER(partition by eventID ORDER BY DATEANDTIME ASC) AS eventDesc_N01,
	Lag(eventDesc, 2) OVER(partition by eventID ORDER BY DATEANDTIME ASC) AS eventDesc_N02,
	Lag(eventDesc, 3) OVER(partition by eventID ORDER BY DATEANDTIME ASC) AS eventDesc_N03,
	Lag(eventDesc, 4) OVER(partition by eventID ORDER BY DATEANDTIME ASC) AS eventDesc_N04,
	Lag(eventDesc, 5) OVER(partition by eventID ORDER BY DATEANDTIME ASC) AS eventDesc_N05,
	Lag(TnF, 1) OVER(partition by eventID ORDER BY DATEANDTIME ASC) AS TnF_N01,
	Lag(TnF, 2) OVER(partition by eventID ORDER BY DATEANDTIME ASC) AS TnF_N02,
	Lag(TnF, 3) OVER(partition by eventID ORDER BY DATEANDTIME ASC) AS TnF_N03,
	Lag(TnF, 4) OVER(partition by eventID ORDER BY DATEANDTIME ASC) AS TnF_N04,
	Lag(TnF, 5) OVER(partition by eventID ORDER BY DATEANDTIME ASC) AS TnF_N05,
	Lag(DATEANDTIME, 1) OVER(partition by eventID ORDER BY DATEANDTIME ASC) AS DATEANDTIME_N01,
	Lag(DATEANDTIME, 3) OVER(partition by eventID ORDER BY DATEANDTIME ASC) AS DATEANDTIME_N03,
	Lag(DATEANDTIME, 5) OVER(partition by eventID ORDER BY DATEANDTIME ASC) AS DATEANDTIME_N05
into TempTable_NuTag
from Test_AlarmTagged
where Test_AlarmTagged.DATEANDTIME <= @OneMinAgo and Test_AlarmTagged.DATEANDTIME >= @TwoMinAgo;


/* Initialise additional computed columns */
alter table TempTable_NuTag
add RepeatAlarm int, ToggleEventA int, ToggleEventB int, NuisanceAlarm int;

/* Update values of newly created columns (currently empty) */
/* Note: dependencies must be execute as a separate query first before generating values reliant on those values */
update TempTable_NuTag set
	RepeatAlarm = (CASE WHEN (eventDesc like eventDesc_N01) AND (TnF = TnF_N01) AND (DATEANDTIME_N01 >= dateadd(minute, -1, DATEANDTIME)) THEN 1 ELSE 0 END),
	
	ToggleEventA = (CASE WHEN (eventDesc like eventDesc_N02) AND (TnF = TnF_N02) AND 
			  (eventDesc_N01 like eventDesc_N03) AND (TnF_N01 = TnF_N03) AND 
			  (DATEANDTIME_N03 >= dateadd(minute, -1, DATEANDTIME)) THEN 1 ELSE 0 END),
	
	ToggleEventB = (CASE WHEN (eventDesc like eventDesc_N02) AND (TnF = TnF_N02) AND 
			  (eventDesc_N01 like eventDesc_N03) AND (TnF_N01 = TnF_N03) AND 
			  (eventDesc like eventDesc_N04) AND (TnF = TnF_N04) AND 
			  (eventDesc_N01 like eventDesc_N05) AND (TnF_N01 = TnF_N05) AND 
			  (DATEANDTIME_N05 >= dateadd(minute, -1, DATEANDTIME)) THEN 1 ELSE 0 END),
			  
	NuisanceAlarm = (CASE WHEN ((RepeatAlarm = 1) OR (ToggleEventA = 1) OR (ToggleEventB = 1)) THEN 1 ELSE 0 END);

/* Update values of newly created columns (currently empty) */
/* Note: dependencies must be execute as a separate query first before generating values reliant on those values */
update TempTable_NuTag set
	NuisanceAlarm = (CASE WHEN ((RepeatAlarm = 1) OR (ToggleEventA = 1) OR (ToggleEventB = 1)) THEN 1 ELSE 0 END);

/* Optional Step */
/* This is just a sure check to ensure that the values have been loaded correctly */
/*
select *
FROM TempTable_NuTag
order by eventID, DATEANDTIME;
*/

/* Recompute DATETIME_LOADED to current time */
Update TempTable_NuTag set DATETIME_LOADED = getdate();

/* Drop Redundant Columns */
ALTER TABLE TempTable_NuTag
DROP COLUMN eventDesc_N01, 
			eventDesc_N02, 
			eventDesc_N03, 
			eventDesc_N04, 
			eventDesc_N05, 
			TnF_N01, 
			TnF_N02, 
			TnF_N03, 
			TnF_N04, 
			TnF_N05, 
			DATEANDTIME_N01, 
			DATEANDTIME_N03, 
			DATEANDTIME_N05;

/* Optional Step */
/* This is just a sure check to ensure that intermediate columns have been deletedcorrectly */
/*
select *
FROM TempTable_NuTag
order by eventID, DATEANDTIME;
*/

/* Transfer records from one table to another */
/* A temporary table is used so that the latest datetime which the data is loaded into the table is recorded */
/* One may remove the N-2min window limit in the transfer of files from 1 table to another as a sure check for any lagging rows of data*/
INSERT INTO Test_Final
SELECT * 
FROM TempTable_NuTag



/* Old records are removed */
/* This is to keep the indexing cost low via keeping the intermediate table size low too */
/* One may remove the N-2min window limit in the transfer of files from 1 table to another as a sure check for any lagging rows of data*/
DELETE FROM Test_AlarmTagged
where Test_AlarmTagged.DATEANDTIME <= @OneMinAgo and Test_AlarmTagged.DATEANDTIME >= @TwoMinAgo;

/* Optional Step */
/* Sure check to ensure that the data has been transferred properly */
/* 
select *
FROM Test_Final
order by eventID, DATEANDTIME;
*/

/* Drop table when run is complete */
DROP TABLE TempTable_NuTag;