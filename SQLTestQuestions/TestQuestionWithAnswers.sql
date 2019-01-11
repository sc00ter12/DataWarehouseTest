USE PersonDatabase;
GO


/*********************
Hello! 

Please use the test data provided in the file 'PersonDatabase' to answer the following
questions. Please also import the dbo.Contracts flat file to a table for use. 

All answers should be written in SQL. 


***********************

QUESTION 1


The table dbo.Person contains basic demographic information. The source system users 
input nicknames as strings inside parenthesis. Write a query or group of queries to 
return the full name and nickname of each person. The nickname should contain only letters 
or be blank if no nickname exists.

**********************/

--Use a CTE to get the space indexes of each name part - this way we don't get extra spaces b/w name parts
-- ASSUMPTION: full names only contain first name and last name
WITH ct_SpaceIndex AS
(
    SELECT
       PersonID
     , PersonName
     , 1 AS StartIndex
     , CHARINDEX(' ',PersonName) AS EndIndex
    FROM PersonDatabase.dbo.Person
    UNION ALL
    SELECT
       PersonID
     , PersonName
     , EndIndex+1 AS StartIndex
     , CASE 
          WHEN CHARINDEX(' ',PersonName,EndIndex+1) = 0 THEN LEN(PersonName)
          ELSE CHARINDEX(' ',PersonName,EndIndex+1)
       END AS EndIndex
    FROM ct_SpaceIndex
    WHERE EndIndex < LEN(PersonName)
)
   , ct_NameParts AS
(
    SELECT
       PersonID
     , CASE 
          WHEN CHARINDEX('(',SUBSTRING(PersonName,StartIndex,EndIndex-StartIndex+1)) > 0 THEN 1
          WHEN CHARINDEX(')',SUBSTRING(PersonName,StartIndex,EndIndex-StartIndex+1)) > 0 THEN 1
          ELSE 0
       END AS NickName
     , REPLACE(REPLACE(LTRIM(RTRIM(SUBSTRING(PersonName,StartIndex,EndIndex-StartIndex+1))),'(',''),')','') AS NamePartClean
     , ROW_NUMBER() OVER ( PARTITION BY PersonID
                                      , CHARINDEX('(',SUBSTRING(PersonName,StartIndex,EndIndex-StartIndex+1))
                                      + CHARINDEX(')',SUBSTRING(PersonName,StartIndex,EndIndex-StartIndex+1))
                               ORDER BY StartIndex ) AS RN
    FROM ct_SpaceIndex
)
SELECT
   MAX(CASE WHEN NickName = 0 AND RN = 1 THEN NamePartClean ELSE '' END) + ' ' +
   MAX(CASE WHEN NickName = 0 AND RN = 2 THEN NamePartClean ELSE '' END) AS FullName
 , MAX(CASE WHEN NickName = 1            THEN NamePartClean ELSE '' END) AS NickName
FROM ct_NameParts
GROUP BY PersonID
ORDER BY PersonID;


/**********************

QUESTION 2


The dbo.Risk table contains risk and risk level data for persons over time for various 
payers. Write a query that returns patient name and their current risk level. 
For patients with multiple current risk levels return only one level so that Gold > Silver > Bronze.


**********************/

--Use a CTE so we can get the row number and be able to filter upon it in the final select
WITH ct_Rows AS
(
    SELECT
       p.PersonName
     , r.RiskLevel
     , ROW_NUMBER() OVER ( PARTITION BY p.PersonName
                               ORDER BY r.RiskDateTime DESC
                                      , CASE
                                           WHEN r.RiskLevel = 'Gold'   THEN 1
                                           WHEN r.RiskLevel = 'Silver' THEN 2
                                           WHEN r.RiskLevel = 'Bronze' THEN 3
                                        END ASC ) AS RN
    FROM PersonDatabase.dbo.Person p
    INNER JOIN PersonDatabase.dbo.Risk r ON p.PersonID = r.PersonID
)
SELECT
   PersonName AS PatientName
 , RiskLevel
FROM ct_Rows
WHERE RN = 1;

/**********************

QUESTION 3

Create a patient matching stored procedure that accepts (first name, last name, dob and sex) as parameters and 
and calculates a match score from the Person table based on the parameters given. If the parameters do not match the existing 
data exactly, create a partial match check using the weights below to assign partial credit for each. Return PatientIDs and the
 calculated match score. Feel free to modify or create any objects necessary in PersonDatabase.  

FirstName 
    Full Credit = 1
    Partial Credit = .5

LastName 
    Full Credit = .8
    Partial Credit = .4

Dob 
    Full Credit = .75
    Partial Credit = .3

Sex 
    Full Credit = .6
    Partial Credit = .25


**********************/

--Function to handle the name splitting
IF OBJECT_ID(N'dbo.fnPersonNameSplit') IS NOT NULL
    DROP FUNCTION dbo.fnPersonNameSplit;
GO
CREATE FUNCTION dbo.fnPersonNameSplit
(
    @inPersonName VARCHAR(255)
)
RETURNS @RtnTable TABLE ( FirstName VARCHAR(255)
                        , LastName  VARCHAR(255)
                        , NickName  VARCHAR(255) )
AS
    --Need to get dbo.Person records broken out into First and Last Name
    -- ASSUMPTION: names, not including the nickname, are in order of FName, LName
BEGIN;
    WITH ct_SpaceIndex AS
    (
        SELECT
           1 AS StartIndex
         , CHARINDEX(' ',@inPersonName) AS EndIndex
        UNION ALL
        SELECT
           EndIndex+1 AS StartIndex
         , CASE 
              WHEN CHARINDEX(' ',@inPersonName,EndIndex+1) = 0 THEN LEN(@inPersonName)
              ELSE CHARINDEX(' ',@inPersonName,EndIndex+1)
           END AS EndIndex
        FROM ct_SpaceIndex
        WHERE EndIndex < LEN(@inPersonName)
    )
       , ct_NameParts AS
    (
        SELECT
           CASE 
              WHEN CHARINDEX('(',SUBSTRING(@inPersonName,StartIndex,EndIndex-StartIndex+1)) > 0 THEN 1
              WHEN CHARINDEX(')',SUBSTRING(@inPersonName,StartIndex,EndIndex-StartIndex+1)) > 0 THEN 1
              ELSE 0
           END AS NickName
         , REPLACE(REPLACE(LTRIM(RTRIM(SUBSTRING(@inPersonName,StartIndex,EndIndex-StartIndex+1))),'(',''),')','') AS NamePartClean
         , ROW_NUMBER() OVER ( PARTITION BY CHARINDEX('(',SUBSTRING(@inPersonName,StartIndex,EndIndex-StartIndex+1))
                                          + CHARINDEX(')',SUBSTRING(@inPersonName,StartIndex,EndIndex-StartIndex+1))
                                   ORDER BY StartIndex ) AS RN
        FROM ct_SpaceIndex
    )
    INSERT INTO @RtnTable ( FirstName
                          , LastName
                          , NickName )
    SELECT
       MAX(CASE WHEN NickName = 0 AND RN = 1 THEN NamePartClean ELSE '' END) AS FirstName
     , MAX(CASE WHEN NickName = 0 AND RN = 2 THEN NamePartClean ELSE '' END) AS LastName
     , MAX(CASE WHEN NickName = 1            THEN NamePartClean ELSE '' END) AS NickName
    FROM ct_NameParts;

    RETURN;
END;
GO

--Function to evalue score by going though string (or date) value
IF OBJECT_ID(N'dbo.fnColumnMatchScore') IS NOT NULL
    DROP FUNCTION fnColumnMatchScore;
GO
CREATE FUNCTION dbo.fnColumnMatchScore
(
    @inColumnDataType VARCHAR(20)
  , @inColumnSource   VARCHAR(255)
  , @inColumnTarget   VARCHAR(255)
  , @inScoreFull      DECIMAL(4,2)
  , @inScorePartial   DECIMAL(4,2)
)
RETURNS @RtnTable TABLE ( Score DECIMAL(4,2) )
AS
BEGIN;
    DECLARE @Score DECIMAL(4,2);

    IF @inColumnSource = @inColumnTarget
    BEGIN;
        --if columns are the same, put in full score to return
        SET @Score = @inScoreFull;
    END;
    ELSE IF @inColumnDataType = 'VARCHAR'
    BEGIN;
        --if target length is longer than source, it does't match
        IF LEN(@inColumnTarget) > LEN(@inColumnSource)
            SET @Score = 0;

        --lets loop through source, see if we match
        --  ex:  @inColumnSource = "MaryBethany"
        --       @inColumnTarget =     "Beth"
        DECLARE @index INT;
        SET @index = 0;

        WHILE @index+LEN(@inColumnTarget) <= LEN(@inColumnSource) AND @Score IS NULL
        BEGIN;
            IF @inColumnTarget = SUBSTRING(@inColumnSource,@index+1,LEN(@inColumnTarget))
                SET @Score = @inScorePartial;

            SET @index += 1;
        END;
    END;
    ELSE IF @inColumnDataType = 'DATE'
    BEGIN;
        DECLARE @workingSource DATE;
        DECLARE @workingTarget DATE;
        
        SELECT
           @workingSource = CAST(@inColumnSource AS DATE)
         , @workingTarget = CAST(@inColumnTarget AS DATE);

        IF DATEPART(YY,@workingSource) = DATEPART(YY,@workingTarget)
            OR DATEPART(MM,@workingSource) = DATEPART(MM,@workingTarget)
            OR DATEPART(DD,@workingSource) = DATEPART(DD,@workingTarget)
            SET @Score = @inScorePartial;
        ELSE
            SET @Score = 0;
    END;

    --Add score to return table
    INSERT INTO @RtnTable ( Score )
    VALUES ( COALESCE(@Score,0.00) );

    RETURN;
END;
GO

--Stored proc
IF OBJECT_ID(N'dbo.ProcGetPatientMatchScore') IS NOT NULL
    DROP PROC dbo.ProcGetPatientMatchScore;
GO

CREATE PROC dbo.ProcGetPatientMatchScore
(
     @inFirstName VARCHAR(255)
   , @inLastName  VARCHAR(255)
   , @inDOB       DATETIME
   , @inSex       VARCHAR(10)
)
AS
--===================================================
-- Name: dbo.ProcGetPatientMatchScore
--
--===================================================
BEGIN;
    SET NOCOUNT ON;

    --Declared Variables to hold our scores
    DECLARE @FirstNameFull    DECIMAL(4,2);
    DECLARE @FirstNamePartial DECIMAL(4,2);
    DECLARE @LastNameFull     DECIMAL(4,2);
    DECLARE @LastNamePartial  DECIMAL(4,2);
    DECLARE @DOBFull          DECIMAL(4,2);
    DECLARE @DOBPartial       DECIMAL(4,2);
    DECLARE @SexFull          DECIMAL(4,2);
    DECLARE @SexPartial       DECIMAL(4,2);

    --Set scores per match values
    SELECT
       @FirstNameFull = 1.0
     , @FirstNamePartial = 0.5
     , @LastNameFull = 0.8
     , @LastNamePartial = 0.4
     , @DOBFull = 0.75
     , @DOBPartial = 0.3
     , @SexFull = 0.6
     , @SexPartial = 0.25;
    
    --Return scores, by PersonID, in descending order
    SELECT
       p.PersonID
     , fn.Score + ln.Score + db.Score + s.Score AS TotalMatchScore
    FROM dbo.Person p
    CROSS APPLY dbo.fnPersonNameSplit ( p.PersonName ) n
    CROSS APPLY dbo.fnColumnMatchScore ( 'VARCHAR', n.FirstName, @inFirstName, @FirstNameFull, @FirstNamePartial ) fn
    CROSS APPLY dbo.fnColumnMatchScore ( 'VARCHAR', n.LastName, @inLastName, @LastNameFull, @LastNamePartial ) ln
    CROSS APPLY dbo.fnColumnMatchScore ( 'DATE', CAST(CAST(p.DateofBirth AS DATE) AS VARCHAR(10)), CAST(CAST(@inDOB AS DATE) AS VARCHAR(10)), @DOBFull, @DOBPartial ) db
    CROSS APPLY dbo.fnColumnMatchScore ( 'VARCHAR', p.Sex, @inSex, @SexFull, @SexPartial ) s
    ORDER BY TotalMatchScore DESC;
END;
GO


/**********************

QUESTION 4

A. Looking at the script 'PersonDatabase', what change(s) to the tables could be made to improve the database structure?  

B. What method(s) could we use to standardize the data allowed in dbo.Person (Sex) to only allow 'Male' or 'Female'?

C. Assuming these tables will grow very large, what other database tools/objects could we use to ensure they remain
efficient when queried?


**********************/

/*
A) 
  1) add primary keys all of the tables.
  2) in the case of dbo.Risk, change the datatype of PersonID to match that in dbo.Person to improve join performance (prevent implicit conversion on joins)
     2a) add a foreign key to reference back to person (trusted keys will help performance as size increases)
  3) for the dbo.Dates table, possibly adding a DateID (vs DateTimeValue) will also help for join performance


B)
   I'd add a check constraint that would only allow the values of Male or Female.  This would cause issues on insert if people don't account for it, but would enfore the standard values
   Will need to update the row for PersonId = 3 to 'Female' from 'F' before the following could be implemented
   ALTER TABLE dbo.Person ADD CONSTRAINT chkPerson_Sex CHECK ( Sex IN ('Male','Female') );

C)
   I'd add indexes upon all the keys that will be joined to.  This can help to ensure we have an index to support queries as needed.
   Setup a maintenance plan (or agent job) that will check fragmentation, and rebuild them as needed.  Likewise, check statistics and update as needed.
*/


/**********************

QUESTION 5

Write a query to return risk data for all patients, all contracts and a moving average of risk for that patient and contract 
in dbo.Risk. 

**********************/


--use a windowing function of AVG to get the moving average
SELECT
   p.PersonId
 , p.PersonName
 , p.Sex
 , p.DateOfBirth
 , p.[Address]
 , r.AttributedPayer
 , r.RiskScore
 , r.RiskLevel
 , r.RiskDateTime
 , AVG(r.RiskScore) OVER ( PARTITION BY p.PersonId
                                      , r.AttributedPayer
                               ORDER BY r.RiskDateTime ) AS RiskMovingAverage
FROM PersonDatabase.dbo.Person p
INNER JOIN PersonDatabase.dbo.Risk r ON p.PersonID = r.PersonID
ORDER BY
   p.PersonId
 , r.RiskDateTime;


/**********************

QUESTION 6

Write script to load the dbo.Dates table with all applicable data elements for dates 
between 1/1/2010 and 500 days past the current date.


**********************/

--Use a recursive CTE to get our date parts
--clean out first (in case there are values in there already)
TRUNCATE TABLE PersonDatabase.dbo.Dates;

DECLARE @StartDate DATETIME;
SET @StartDate = '2010-01-01';

WITH ct_Dates AS
(
    SELECT 
       @StartDate AS DateValue
     , DATEPART(DD,@StartDate) AS DateDayofMonth
     , DATEPART(DY,@StartDate) AS DateDayofYear
     , DATEPART(QQ,@StartDate) AS DateQuarter
     , DATENAME(WK,@StartDate) AS DateWeekdayName
     , DATENAME(MM,@StartDate) AS DateMonthName
     , CAST(DATEPART(YY,@StartDate) AS CHAR(4)) + RIGHT('0' + CAST(DATEPART(MM,@StartDate) AS VARCHAR(2)),2) AS DateYearMonth
    UNION ALL
    SELECT
       DATEADD(DD,1,DateValue) AS DateValue
     , DATEPART(DD,DATEADD(DD,1,DateValue)) AS DateDayofMonth
     , DATEPART(DY,DATEADD(DD,1,DateValue)) AS DateDayofYear
     , DATEPART(QQ,DATEADD(DD,1,DateValue)) AS DateQuarter
     , DATENAME(WK,DATEADD(DD,1,DateValue)) AS DateWeekdayName
     , DATENAME(MM,DATEADD(DD,1,DateValue)) AS DateMonthName
     , CAST(DATEPART(YY,DATEADD(DD,1,DateValue)) AS CHAR(4)) + RIGHT('0' + CAST(DATEPART(MM,DATEADD(DD,1,DateValue)) AS VARCHAR(2)),2) AS DateYearMonth
    FROM ct_Dates
    WHERE CAST(DATEADD(DD,1,DateValue) AS DATE) <= CAST(DATEADD(DD,500,GETDATE()) AS DATE)
)
INSERT INTO PersonDatabase.dbo.Dates ( DateValue
                                     , DateDayofMonth
                                     , DateDayofYear
                                     , DateQuarter
                                     , DateWeekdayName
                                     , DateMonthName
                                     , DateYearMonth )
SELECT
   DateValue
 , DateDayOfMonth
 , DateDayofYear
 , DateQuarter
 , DateWeekdayName
 , DateMonthName
 , DateYearMonth
FROM ct_Dates
OPTION (MAXRECURSION 4000);


/**********************

QUESTION 7

Please import the data from the flat file dbo.Contracts.txt to a table to complete this question. 

Using the data in dbo.Contracts, create a query that returns 

    (PersonID, AttributionStartDate, AttributionEndDate) 

The data should be structured so that rows with contiguous ranges are merged into a single row. Rows that contain a 
break in time of 1 day or more should be entered as a new record in the output. Restarting a row for a new 
month or year is not necessary.

Use the dbo.Dates table if helpful.

**********************/



--Create table so we can BULK INSERT into it
IF OBJECT_ID(N'PersonDatabase.dbo.Contracts') IS NOT NULL
    DROP TABLE PersonDatabase.dbo.Contracts;
CREATE TABLE PersonDatabase.dbo.Contracts ( PersonID          INT      NULL
                                          , ContractStartDate DATETIME NULL
                                          , ContractEndDate   DATETIME NULL );

--Load it from TXT File
BULK INSERT dbo.Contracts
FROM 'c:\TEMP\dbo.Contracts.txt'
WITH ( FIELDTERMINATOR = '\t'  --tab separator
     , ROWTERMINATOR = '\r'    --new lines are in *nix systems (just LF)
     , FIRSTROW = 2 );         --skip headers

--Chained CTE
WITH ct_UnpivotDates AS
(   --Unpivot dates so we can get proper ordering
    SELECT
       PersonId
     , ROW_NUMBER() OVER ( PARTITION BY PersonId
                               ORDER BY ContractStartDate
                                      , ContractEndDate ) AS RN
     , 'Start' AS DateType
     , ContractStartDate AS ContractDate
    FROM dbo.Contracts c
    UNION ALL
    SELECT
       PersonId
     , ROW_NUMBER() OVER ( PARTITION BY PersonId
                               ORDER BY ContractStartDate
                                      , ContractEndDate ) AS RN
     , 'End' AS DateType
     , ContractEndDate AS ContractDate
    FROM dbo.Contracts c
)
   , ct_Rows AS
(   --Now with it ordered, lets figure out which ones have dates that are the same (start or end)
    SELECT
       PersonID
     , RN
     , DateType
     , ContractDate
     , CASE 
          WHEN DATEDIFF(DD,ContractDate,LAG(ContractDate) OVER ( PARTITION BY PersonId
                                                                     ORDER BY ContractDate
                                                                            , DateType )) = 0 THEN 0
          ELSE 1
       END AS GroupAdd
    FROM ct_UnpivotDates 
)
   , ct_Dates AS
(   --Based on the ones that are same, get a new group number
    SELECT
       PersonID
     , RN
     , DateType
     , ContractDate
     , CASE 
          WHEN GroupAdd = 0 AND DateType = 'Start' THEN LAG(RN) OVER ( PARTITION BY PersonId
                                                                        ORDER BY ContractDate
                                                                               , DateType )
          ELSE RN
       END AS RNGroup
    FROM ct_Rows
)
   , ct_DateRegroup AS
(   --Reset group number for start/end date (that didn't have the overlap)
    SELECT
       PersonID
     , RN
     , MIN(RNGroup) AS rnNew
    FROM ct_Dates
    GROUP BY
       PersonID
     , RN
)  --final select with start and end dates, by group
SELECT
   dr.PersonId
 , MIN(CASE WHEN d.DateType = 'Start' THEN d.ContractDate ELSE '21000101' END) AS ContractStartDate
 , MAX(CASE WHEN d.DateType = 'End'   THEN d.ContractDate ELSE '19000101' END) AS ContractEndDate
FROM ct_DateRegroup dr
INNER JOIN ct_Dates d ON dr.PersonID = d.PersonID
   AND dr.RN = d.RN
GROUP BY
   dr.PersonId
 , dr.rnNew
ORDER BY 
   dr.PersonID
 , ContractStartDate;
