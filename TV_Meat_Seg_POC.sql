
###########################################################################################################
##############################      POC MEAT  ##########################################
###########################################################################################################


--------------------------------
--Creating base tables for shops
--------------------------------------------------------------------------------------------------------------------------------------------------
--STEP 1 : Create base table for Total Supermarket & particular products for last 2 years
--------------------------------------------------------------------------------------------------------------------------------------------------


create or replace table `gcp-wow-supers-rtla-qtm-dev.TrashPanda.poc_meat_txns_TV` as

SELECT 
    BasketKey,
    BusinessDate,
    SiteNumber,	
    CRN,
    ISS.ProductNumber,
    ISS.DepartmentDescription,
    ISS.CategoryDescription,
    ISS.SubCategoryDescription,
    C.key AS Dendro_Segment,
    c.GroupName AS Dendro_Group_Name,
    ISS.SegmentDescription,
    ISS.ArticleDescription,
    ISS.PromotionWeekEndDate,
    BMP,
    LS,
    NeedsType1 AS NEEDS, 
    CASE 
      WHEN C.ProteinGroup IS NULL THEN 'Other'
      WHEN C.ProteinGroup = 'Poultry' THEN 'POULTRY'
      WHEN C.ProteinGroup = 'Red meat' THEN 'RED MEAT'
       ELSE C.ProteinGroup END CATEGORY,
    CASE 
        WHEN BusinessDate BETWEEN DATE_SUB('2022-09-20', INTERVAL 7*52+1 DAY) AND DATE_SUB('2022-09-20', INTERVAL 7*26 DAY) THEN 'L26W' 
        WHEN BusinessDate BETWEEN DATE_SUB('2022-09-20', INTERVAL 7*26+1 DAY) AND '2022-09-20' THEN 'R26W'
        ELSE 'LY' END TIME_PERIOD,
     CASE 
        WHEN BusinessDate BETWEEN DATE_SUB('2022-09-20', INTERVAL 7*104+1 DAY) AND DATE_SUB('2022-09-20', INTERVAL 7*52 DAY) THEN 'LY' 
        WHEN BusinessDate BETWEEN DATE_SUB('2022-09-20', INTERVAL 7*52+1 DAY) AND '2022-09-20' THEN 'TY'
        ELSE 'ERROR' END TIME_PERIOD2,       
    SUM(TotalAmountIncludingGST) AS SALES,  
    SUM(Volume) AS VOLUME,
    SUM(RetailQuantity) AS QTY

FROM `gcp-wow-supers-rtla-qtm-prod.customer_base_table.CustomerBaseTransaction` AS ISS

INNER JOIN  `gcp-wow-supers-rtla-qtm-dev.202207_Dendrogram_Protein.TREE_input_all` AS ASSORT --corrected assortments
		ON ISS.Article  = ASSORT.ARTICLE_NBR

LEFT JOIN gcp-wow-supers-rtla-qtm-dev.Integrated_Protein_Strategy.ProteinGrouping C
	on ASSORT.Category = C.key 

WHERE CRN IS NOT NULL
AND BusinessDate between DATE_SUB('2022-09-20', INTERVAL 7*104+1 DAY) AND '2022-09-20'
AND  iss.TotalAmountIncludingGST > 0
AND  iss.RetailQuantity > 0
AND SalesOrganisation = '1005'

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16, 17, 18
;


-- select category, sum(SALES) from `gcp-wow-supers-rtla-qtm-dev.TrashPanda.poc_meat_txns_TV` group by 1

--check volumes by protein type

SELECT TIME_PERIOD2, CATEGORY, SUM(VOLUME)
FROM gcp-wow-supers-rtla-qtm-dev.TrashPanda.poc_meat_txns_TV
GROUP BY 1, 2;


------------------------------------------------------------------------------
--STEP 2 : Create rank of customers - based on the volume -> separately for TY and LLY
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------   

--TY v LY

create or replace table `gcp-wow-supers-rtla-qtm-dev.TrashPanda.poc_crn_rank_LY_TV` as
with tab as
(
select 
CRN,
TIME_PERIOD2 as TIME_PERIOD, --> TY vs LY 
CATEGORY,
SUM(SALES) AS SALES,  
SUM(VOLUME) AS VOLUME,
SUM(QTY) AS QTY
FROM `gcp-wow-supers-rtla-qtm-dev.TrashPanda.poc_meat_txns_TV` 
GROUP BY 1,2,3
), tab2 as  
(
SELECT
CRN,
TIME_PERIOD,
SUM(CASE WHEN CATEGORY='POULTRY' THEN VOLUME ELSE 0 END) AS POULTRY_VOLUME,
SUM(CASE WHEN CATEGORY='RED MEAT' THEN VOLUME ELSE 0 END) AS REDMEAT_VOLUME,
SUM(Sales) AS SALES,
SUM(CASE WHEN CATEGORY='POULTRY' THEN Sales ELSE 0 END) AS POULTRY_SALES,
SUM(CASE WHEN CATEGORY='RED MEAT' THEN Sales ELSE 0 END) AS REDMEAT_SALES,
FROM tab
GROUP BY 1,2
)
SELECT
CRN,
TIME_PERIOD,
POULTRY_VOLUME,
REDMEAT_VOLUME,
SALES,
POULTRY_SALES,
REDMEAT_SALES,
ROW_NUMBER() OVER (PARTITION BY TIME_PERIOD ORDER BY POULTRY_VOLUME) AS RN_POULTRY,
RANK() OVER (PARTITION BY TIME_PERIOD ORDER BY POULTRY_VOLUME) AS RANK_POULTRY,
ROW_NUMBER() OVER (PARTITION BY TIME_PERIOD ORDER BY REDMEAT_VOLUME) AS RN_REDMEAT,
RANK() OVER (PARTITION BY TIME_PERIOD ORDER BY REDMEAT_VOLUME) AS RANK_REDMEAT,
COUNT(CRN) OVER (PARTITION BY TIME_PERIOD) AS CUSTOMER
FROM tab2
;

select TIME_PERIOD, count(distinct crn)
from gcp-wow-supers-rtla-qtm-dev.TrashPanda.poc_crn_rank_LY_TV
group by 1


/*
select * from `gcp-wow-supers-rtla-qtm-dev.TrashPanda.poc_crn_rank` where TIME_PERIOD='R26W' and POULTRY_VOLUME <= 0 limit 10
 order by TIME_PERIOD, rank_poultry  limit 1000
select TIME_PERIOD, count(*), count(distinct CRN) from `gcp-wow-supers-rtla-qtm-dev.TrashPanda.poc_crn_rank` group by 1
*/

---------------------
--average how many kg customers buy
select  
TIME_PERIOD,
--avg(POULTRY_VOLUME) avg_volume,
avg(REDMEAT_VOLUME) avg_volume2,
count(CRN) as Customers,
from `gcp-wow-supers-rtla-qtm-dev.TrashPanda.poc_crn_rank_TY`
where -- POULTRY_VOLUME > 0 
 REDMEAT_VOLUME > 0
group by 1 order by 1 desc
-------------------

--V1 MEAT SEG

CREATE OR REPLACE TABLE `gcp-wow-supers-rtla-qtm-dev.TrashPanda.Meat_Seg_CRN_v1` AS

with tab as (
SELECT
t.*,
(RN_POULTRY/CUSTOMER) AS PERCENT_POULTRY,
(RN_REDMEAT/CUSTOMER) AS PERCENT_REDMEAT,
FROM `gcp-wow-supers-rtla-qtm-dev.TrashPanda.poc_crn_rank_LY_TV` as t
), tab2 as 
(
SELECT 
tt.*,
CASE 
  WHEN PERCENT_POULTRY < 0.5 THEN 'LIGHT POULTRY'
  WHEN PERCENT_POULTRY >= 0.5 AND PERCENT_POULTRY < 0.8 THEN 'MEDIUM POULTRY' 
  WHEN PERCENT_POULTRY >= 0.8 AND PERCENT_POULTRY <=1 THEN 'HEAVY POULTRY' ELSE 'ERROR' END GROUP_POULTRY,

CASE 
  WHEN PERCENT_REDMEAT < 0.5 THEN 'LIGHT RED MEAT'
  WHEN PERCENT_REDMEAT >= 0.5 AND PERCENT_REDMEAT < 0.8 THEN 'MEDIUM RED MEAT' 
  WHEN PERCENT_REDMEAT >= 0.8 AND PERCENT_REDMEAT <=1 THEN 'HEAVY RED MEAT' ELSE 'ERROR' END GROUP_REDMEAT,

CASE 
  WHEN PERCENT_POULTRY < 0.5 THEN 1
  WHEN PERCENT_POULTRY >= 0.5 AND PERCENT_POULTRY < 0.8 THEN 2 
  WHEN PERCENT_POULTRY >= 0.8 AND PERCENT_POULTRY <=1 THEN 3 ELSE NULL END GRADING_POULTRY,

CASE 
  WHEN PERCENT_REDMEAT < 0.5 THEN 1
  WHEN PERCENT_REDMEAT >= 0.5 AND PERCENT_REDMEAT < 0.8 THEN 2 
  WHEN PERCENT_REDMEAT >= 0.8 AND PERCENT_REDMEAT <=1 THEN 3 ELSE NULL END GRADING_REDMEAT,

-- CASE 
--   WHEN PERCENT_POULTRY < 0.25 THEN 'LOW POULTRY'
--   WHEN PERCENT_POULTRY >= 0.25 AND PERCENT_POULTRY < 0.75 THEN 'MEDIUM POULTRY' 
--   WHEN PERCENT_POULTRY >= 0.75 AND PERCENT_POULTRY <=1 THEN 'HIGH POULTRY' ELSE 'ERROR' END GROUP_POULTRY,

-- CASE 
--   WHEN PERCENT_REDMEAT < 0.25 THEN 'LOW RED MEAT'
--   WHEN PERCENT_REDMEAT >= 0.25 AND PERCENT_REDMEAT < 0.75 THEN 'MEDIUM RED MEAT' 
--   WHEN PERCENT_REDMEAT >= 0.75 AND PERCENT_REDMEAT <=1 THEN 'HIGH RED MEAT' ELSE 'ERROR' END GROUP_REDMEAT,

-- CASE 
--   WHEN PERCENT_POULTRY < 0.25 THEN 1
--   WHEN PERCENT_POULTRY >= 0.25 AND PERCENT_POULTRY < 0.75 THEN 2 
--   WHEN PERCENT_POULTRY >= 0.75 AND PERCENT_POULTRY <=1 THEN 3 ELSE NULL END GRADING_POULTRY,

-- CASE 
--   WHEN PERCENT_REDMEAT < 0.25 THEN 1
--   WHEN PERCENT_REDMEAT >= 0.25 AND PERCENT_REDMEAT < 0.75 THEN 2 
--   WHEN PERCENT_REDMEAT >= 0.75 AND PERCENT_REDMEAT <=1 THEN 3 ELSE NULL END GRADING_REDMEAT,  

-- CASE 
--   WHEN PERCENT_POULTRY < 0.25 AND PERCENT_REDMEAT < 0.25 THEN 'OCCASIONAL CONSUMERS'

--   WHEN PERCENT_POULTRY < 0.25 AND PERCENT_REDMEAT >= 0.25 AND PERCENT_REDMEAT < 0.75 THEN 'MEDIUM RED MEAT CONSUMERS'
--   WHEN PERCENT_POULTRY < 0.25 AND PERCENT_REDMEAT > 0.75 THEN 'HEAVY RED MEAT CONSUMERS'

--   WHEN PERCENT_REDMEAT < 0.25 AND PERCENT_POULTRY >= 0.25 AND PERCENT_POULTRY < 0.75 THEN 'MEDIUM POULTRY CONSUMERS'
--   WHEN PERCENT_REDMEAT < 0.25 AND PERCENT_POULTRY > 0.75 THEN 'HEAVY POULTRY CONSUMERS'  

--   WHEN PERCENT_POULTRY >= 0.25 AND PERCENT_REDMEAT >= 0.25 AND PERCENT_REDMEAT < 0.75 THEN 'MEDIUM RED MEAT - MEDIUM HIGH POULTRY CONSUMER'
--   WHEN PERCENT_POULTRY >= 0.25 AND PERCENT_REDMEAT > 0.75 THEN 'HEAVY RED MEAT - MEDIUM HIGH POULTRY CONSUMER'
--   ELSE 'NO SEGMENT' END AS PROTEIN_SEGMENT,

CASE 
  WHEN PERCENT_POULTRY < 0.25 AND PERCENT_REDMEAT < 0.25 THEN 'OCCASIONAL CONSUMERS'

  WHEN PERCENT_POULTRY < 0.25 AND PERCENT_REDMEAT >= 0.25 AND PERCENT_REDMEAT < 0.75 THEN 'MEDIUM RED MEAT - LOW POULTRY'
  WHEN PERCENT_POULTRY < 0.25 AND PERCENT_REDMEAT > 0.75 THEN 'HIGH RED MEAT - LOW POULTRY'

  WHEN PERCENT_REDMEAT < 0.25 AND PERCENT_POULTRY >= 0.25 AND PERCENT_POULTRY < 0.75 THEN 'LOW RED MEAT - MEDIUM POULTRY'
  WHEN PERCENT_REDMEAT < 0.25 AND PERCENT_POULTRY > 0.75 THEN 'LOW RED MEAT - HIGH POULTRY'  

  WHEN PERCENT_POULTRY >= 0.25 AND PERCENT_POULTRY < 0.75 AND PERCENT_REDMEAT >= 0.25 AND PERCENT_REDMEAT < 0.75 THEN 'MEDIUM RED MEAT - MEDIUM POULTRY'
  WHEN PERCENT_POULTRY >= 0.25 AND PERCENT_POULTRY < 0.75 AND PERCENT_REDMEAT > 0.75 THEN 'HIGH RED MEAT - MEDIUM POULTRY'

  WHEN PERCENT_POULTRY > 0.75 AND PERCENT_REDMEAT >= 0.25 AND PERCENT_REDMEAT < 0.75 THEN 'MEDIUM RED MEAT - HIGH POULTRY'
  WHEN PERCENT_POULTRY > 0.75 AND PERCENT_REDMEAT > 0.75 THEN 'HIGH RED MEAT - HIGH POULTRY'
  ELSE 'NO SEGMENT' END AS PROTEIN_SEGMENT,  

CASE 
  WHEN PERCENT_REDMEAT < 0.25 THEN 'OCCASION RED'
  WHEN PERCENT_REDMEAT >= 0.25 AND PERCENT_REDMEAT < 0.75 THEN 'MEDIUM RED' 
  WHEN PERCENT_REDMEAT >= 0.75 AND PERCENT_REDMEAT <=1 THEN 'HEAVY RED' ELSE 'ERROR' END SEGMENT_REDMEAT,

CASE WHEN POULTRY_VOLUME <= 0 THEN '0.<= 0'
    WHEN POULTRY_VOLUME > 0 AND POULTRY_VOLUME <= 1.5 THEN '1. <=1.5'
    WHEN POULTRY_VOLUME > 1.5 AND POULTRY_VOLUME <= 5 THEN '2. <=5'
    WHEN POULTRY_VOLUME > 5 AND POULTRY_VOLUME <= 10 THEN '3. <=10'
    WHEN POULTRY_VOLUME > 10 THEN '4. >10' ELSE 'ERROR' 
    END POULTRY_VOLUME_GROUP,
CASE WHEN REDMEAT_VOLUME <= 0 THEN '0.<= 0'
    WHEN REDMEAT_VOLUME > 0 AND REDMEAT_VOLUME <= 1.5 THEN '1. <=1.5'
    WHEN REDMEAT_VOLUME > 1.5 AND REDMEAT_VOLUME <= 5 THEN '2. <=5'
    WHEN REDMEAT_VOLUME > 5 AND REDMEAT_VOLUME <= 10 THEN '3. <=10'
    WHEN REDMEAT_VOLUME > 10 THEN '4. >10' ELSE 'ERROR' 
    END REDMEAT_VOLUME_GROUP,
FROM tab as tt
)
SELECT
CRN,
TIME_PERIOD,
GROUP_POULTRY,
GROUP_REDMEAT,
GRADING_POULTRY, 
GRADING_REDMEAT,
PROTEIN_SEGMENT,
POULTRY_VOLUME,
REDMEAT_VOLUME

FROM tab2
GROUP BY 1,2,3, 4, 5, 6, 7, 8, 9;

--CHECK THRESHOLD

--POULTRY

SELECT TIME_PERIOD, GRADING_POULTRY, MIN(POULTRY_VOLUME), MAX(POULTRY_VOLUME)
FROM gcp-wow-supers-rtla-qtm-dev.TrashPanda.Meat_Seg_CRN_v1
WHERE TIME_PERIOD = 'LY'
GROUP BY 1, 2;

SELECT TIME_PERIOD, GRADING_POULTRY, MIN(POULTRY_VOLUME), MAX(POULTRY_VOLUME)
FROM gcp-wow-supers-rtla-qtm-dev.TrashPanda.Meat_Seg_CRN_v1
WHERE TIME_PERIOD = 'TY'
GROUP BY 1, 2;

--RM

SELECT TIME_PERIOD, GRADING_REDMEAT, AVG(REDMEAT_VOLUME), MIN(REDMEAT_VOLUME), MAX(REDMEAT_VOLUME)
FROM gcp-wow-supers-rtla-qtm-dev.TrashPanda.Meat_Seg_CRN_v1
WHERE TIME_PERIOD = 'LY'
GROUP BY 1, 2;

SELECT TIME_PERIOD, GRADING_REDMEAT, AVG(REDMEAT_VOLUME), MIN(REDMEAT_VOLUME), MAX(REDMEAT_VOLUME)
FROM gcp-wow-supers-rtla-qtm-dev.TrashPanda.Meat_Seg_CRN_v1
WHERE TIME_PERIOD = 'TY'
GROUP BY 1, 2;

--CHECK PROPORTION OF NO SEGMENT CUSTOMERS

SELECT 
  TIME_PERIOD,
  PROTEIN_SEGMENT,
  COUNT(DISTINCT CRN)
FROM `gcp-wow-supers-rtla-qtm-dev.TrashPanda.Meat_Seg_CRN_v1`
GROUP BY 1, 2


SELECT 
  TIME_PERIOD,
  GROUP_POULTRY,
  COUNT(DISTINCT CRN)
FROM `gcp-wow-supers-rtla-qtm-dev.TrashPanda.Meat_Seg_CRN_v1`
GROUP BY 1, 2

SELECT *
FROM `gcp-wow-supers-rtla-qtm-dev.TrashPanda.Meat_Seg_CRN_v1`
WHERE PROTEIN_SEGMENT = 'NO SEGMENT'
LIMIT 10

SELECT *, (RANK_POULTRY/CUSTOMER) AS PERCENT_POULTRY,
(RANK_REDMEAT/CUSTOMER) AS PERCENT_REDMEAT,
FROM gcp-wow-supers-rtla-qtm-dev.TrashPanda.poc_crn_rank_LY_TV
WHERE CRN = '1100000000108471623'
AND TIME_PERIOD = 'LY'
;

--TEST PROPORTION OF POULTRY CUSTOMERS

SELECT
  time_period
  , CASE 
    WHEN PROTEIN_SEGMENT LIKE '%MEDIUM RED MEAT%' THEN 'MEDIUM RED MEAT'
    WHEN PROTEIN_SEGMENT LIKE '%HEAVY RED MEAT%' THEN 'HEAVY RED MEAT'
    WHEN PROTEIN_SEGMENT LIKE '%OCCASION%' OR PROTEIN_SEGMENT IN ('MEDIUM POULTRY CONSUMERS', 'HEAVY POULTRY CONSUMERS') THEN 'LOW RED MEAT' ELSE NULL END AS RM_SEG
    , GROUP_REDMEAT
    , COUNT(DISTINCT CRN)

FROM `gcp-wow-supers-rtla-qtm-dev.TrashPanda.Meat_Seg_CRN_v1`

GROUP BY 1, 2, 3
;


--TRANSITION

CREATE OR REPLACE TABLE `gcp-wow-supers-rtla-qtm-dev.TrashPanda.Meat_Seg_Transition_CRN_v1` AS

SELECT 
  A.CRN
  , A.PROTEIN_SEGMENT AS PROTEIN_SEGMENT_TY
  , B.PROTEIN_SEGMENT AS PROTEIN_SEGMENT_LY

FROM (

  SELECT 
      CRN, PROTEIN_SEGMENT

  FROM gcp-wow-supers-rtla-qtm-dev.TrashPanda.Meat_Seg_CRN_v1

  WHERE TIME_PERIOD = 'TY'
    AND PROTEIN_SEGMENT <> 'NO SEGMENT'

  GROUP BY 1, 2

) A 

INNER JOIN (

  SELECT 
      CRN, PROTEIN_SEGMENT

  FROM gcp-wow-supers-rtla-qtm-dev.TrashPanda.Meat_Seg_CRN_v1

  WHERE TIME_PERIOD = 'LY'
    AND PROTEIN_SEGMENT <> 'NO SEGMENT'

  GROUP BY 1, 2

) B
  ON A.CRN = B.CRN
;

CREATE OR REPLACE TABLE `gcp-wow-supers-rtla-qtm-dev.TrashPanda.Meat_Seg_Transition_V2` AS

SELECT 
  A.CRN

  , B.GROUP_POULTRY
  , B.GROUP_REDMEAT
  , B.GRADING_POULTRY 
  , B.GRADING_REDMEAT

  , Total_Volume

  , CASE WHEN A.GRADING_POULTRY - B.GRADING_POULTRY > 0 THEN 1 ELSE 0 END AS POULTRY_CONSUMPTION_GRADING_INCREASE
  , CASE WHEN A.GRADING_REDMEAT - B.GRADING_REDMEAT < 0 THEN 1 ELSE 0 END AS RED_MEAT_CONSUMPTION_GRADING_DECREASE

FROM (

  SELECT 
      CRN
      , PROTEIN_SEGMENT
      , GROUP_POULTRY
      , GROUP_REDMEAT
      , GRADING_POULTRY 
      , GRADING_REDMEAT
      , POULTRY_VOLUME + REDMEAT_VOLUME AS Total_Volume

  FROM gcp-wow-supers-rtla-qtm-dev.TrashPanda.Meat_Seg_CRN_v1

  WHERE TIME_PERIOD = 'TY'
    AND PROTEIN_SEGMENT <> 'NO SEGMENT'

  GROUP BY 1, 2, 3, 4, 5, 6, 7

) A 

INNER JOIN (

  SELECT 
      CRN
      , PROTEIN_SEGMENT
      , GROUP_POULTRY
      , GROUP_REDMEAT
      , GRADING_POULTRY 
      , GRADING_REDMEAT

  FROM gcp-wow-supers-rtla-qtm-dev.TrashPanda.Meat_Seg_CRN_v1

  WHERE TIME_PERIOD = 'LY'
    AND PROTEIN_SEGMENT <> 'NO SEGMENT'

  GROUP BY 1, 2, 3, 4, 5, 6

) B
  ON A.CRN = B.CRN
;

--ly ty distribution

SELECT 
  TIME_PERIOD
  , GROUP_POULTRY
  , GROUP_REDMEAT
  , COUNT(DISTINCT CRN) AS CUST_COUNT

FROM gcp-wow-supers-rtla-qtm-dev.TrashPanda.Meat_Seg_CRN_v1

GROUP BY 1, 2, 3

--PROPORTION OF UPGRADING POULTRY CONSUMPTION

SELECT
  GROUP_POULTRY
  , GROUP_REDMEAT
  , COUNT(DISTINCT CRN) AS TOTAL_CUST_COUNT
  -- , COUNT(DISTINCT CASE WHEN POULTRY_CONSUMPTION_GRADING_INCREASE = 1 THEN CRN ELSE NULL END) AS TOTAL_CUST_INCREASING_POULTRY_GRADING
  -- , COUNT(DISTINCT CASE WHEN POULTRY_CONSUMPTION_GRADING_INCREASE = 1 AND (RED_MEAT_CONSUMPTION_GRADING_DECREASE = 1 OR GRADING_REDMEAT = 1 AND GRADING_POULTRY <> 1) THEN CRN ELSE NULL END) AS TOTAL_CUST_INCREASING_POULTRY_DECREASING_RED_MEAT
  -- , COUNT(DISTINCT CASE WHEN POULTRY_CONSUMPTION_GRADING_INCREASE = 1 AND (RED_MEAT_CONSUMPTION_GRADING_DECREASE = 1 OR GRADING_REDMEAT = 1 AND GRADING_POULTRY <> 1) THEN CRN ELSE NULL END) / COUNT(DISTINCT CRN) AS PROP_INCREASING_POULTRY_DECREASING_RED_MEAT

  , COUNT(DISTINCT CASE WHEN POULTRY_CONSUMPTION_GRADING_INCREASE = 1 THEN CRN ELSE NULL END) / COUNT(DISTINCT CRN) AS PROPORTION_POULTRY_UPGRADING
  , COUNT(DISTINCT CASE WHEN POULTRY_CONSUMPTION_GRADING_INCREASE = 1 THEN CRN ELSE NULL END) / sum(COUNT(DISTINCT CRN)) over() AS PROPORTION_OF_CUSTS

FROM `gcp-wow-supers-rtla-qtm-dev.TrashPanda.Meat_Seg_Transition_V2`

GROUP BY 1, 2

--CREATE NEEDS TYPE 2

CREATE OR REPLACE TABLE `gcp-wow-supers-rtla-qtm-dev.TrashPanda.Meat_Seg_CustomerBaseNeedsTy2TMP`
AS
-- To get Needs segments (Left join to call missing ones unsegmented) - Type 2
  SELECT 
    NEEDS2.*
    ,IFNULL(MAP.groupname, 'Unsegmented_group') as NEEDS_GROUPED_TY2
  FROM (
    SELECT CRN, allocated, pw_end_Date AS EffectiveStartDate,
    IFNULL(
    (
      SELECT 
      MIN(pw_end_Date)
      FROM `gcp-wow-rwds-ai-beh-seg-prod.segmentations.super`
      WHERE pw_end_Date > edt.pw_end_Date
      AND CRN = edt.CRN), MAX(pw_end_Date) + 100000
    )AS EffectiveEndDate
    FROM `gcp-wow-rwds-ai-beh-seg-prod.segmentations.super` edt
    GROUP BY 1,2,3
  ) NEEDS2

  -- To get the most recent Needs segment names for Supers
  LEFT JOIN (select * from `gcp-wow-rwds-ai-beh-seg-prod.segmentations.bhnames` where banner='super') MAP 
  ON NEEDS2.allocated = MAP.allocated
;

--Profiling, benchmarked against total Protein consumers
--Profiling is based on LY

CREATE OR REPLACE TABLE `gcp-wow-supers-rtla-qtm-dev.TrashPanda.Meat_Seg_Base_Profiling` AS 

SELECT 
  A.*
  , COALESCE(BMP.PriceSegmentDescription, 'NO SEGMENT') AS BMP
  , COALESCE(LS.LifeStageSegmentDescription, 'NO SEGMENT') AS LS
  , COALESCE(NEEDS2.NEEDS_GROUPED_TY2, 'NO SEGMENT') AS CGEST

FROM gcp-wow-supers-rtla-qtm-dev.TrashPanda.Meat_Seg_Transition_V2 A 

LEFT JOIN `gcp-wow-ent-im-tbl-prod.adp_quantium_wow_commercials_view.qtm_cust_price_segment_hist_v` AS BMP
  ON A.CRN = BMP.CRN
  AND DATE_SUB('2022-09-20', INTERVAL 7*26 DAY) BETWEEN BMP.DateEffectiveFrom AND BMP.DateEffectiveTo
                                      
LEFT JOIN `gcp-wow-ent-im-tbl-prod.adp_quantium_wow_commercials_view.qtm_cust_lifestage_segment_hist_v`  AS LS
  ON A.CRN = LS.CRN
  AND DATE_SUB('2022-09-20', INTERVAL 7*26 DAY) BETWEEN LS.DateEffectiveFrom AND LS.DateEffectiveTo

LEFT JOIN `gcp-wow-supers-rtla-qtm-dev.TrashPanda.Meat_Seg_CustomerBaseNeedsTy2TMP` NEEDS2
  ON Cast(NEEDS2.CRN as String) = A.CRN
    AND DATE_SUB('2022-09-20', INTERVAL 7*26 DAY) > NEEDS2.EffectiveStartDate 
    AND DATE_SUB('2022-09-20', INTERVAL 7*26 DAY) <= NEEDS2.EffectiveEndDate

--Top switching in Poultry segments
--PARTITION BY TargetGroupName 

CREATE OR REPLACE TABLE gcp-wow-supers-rtla-qtm-dev.Integrated_Protein_Strategy.Top_Switching_Assortment_By_Meat_Seg AS 

SELECT 
				GROUP_POULTRY
        , GROUP_REDMEAT

        , TARGET_ASSORTMENT
				, TargetGroupName
				, TargetProteinGroup

				,	SUM(SWITCHING_IN_SALES) AS SWITCHING_IN_SALES_

				, ROW_NUMBER() OVER (PARTITION BY GROUP_POULTRY, GROUP_REDMEAT ORDER BY SUM(SWITCHING_IN_SALES) DESC) AS RANK_WITHIN_POULTRY_GROUP

FROM gcp-wow-supers-rtla-qtm-dev.Integrated_Protein_Strategy.Protein_Switching_Sales_Volume_N2N_ProteinGroup_Type A

LEFT JOIN gcp-wow-supers-rtla-qtm-dev.TrashPanda.Meat_Seg_Transition_V2 B 
  ON A.CRN = B.CRN

WHERE TargetProteinGroup = 'Poultry'
  AND POULTRY_CONSUMPTION_GRADING_INCREASE = 1

GROUP BY 1, 2, 3, 4, 5

;

-- volume proportion by protein type - LY

SELECT
  				GROUP_POULTRY
        , GROUP_REDMEAT
        , CATEGORY
        , SUM(VOLUME) AS TOT_VOL

FROM gcp-wow-supers-rtla-qtm-dev.TrashPanda.poc_meat_txns_TV A

INNER JOIN gcp-wow-supers-rtla-qtm-dev.TrashPanda.Meat_Seg_Transition_V2 B 
  ON A.CRN = B.CRN

WHERE TIME_PERIOD2 = 'LY'

GROUP BY 1, 2, 3

--check volume of Light Poultry Heavy Red meat

SELECT
  				A.CRN
        , SUM(VOLUME) AS TOT_VOL

FROM gcp-wow-supers-rtla-qtm-dev.TrashPanda.poc_meat_txns_TV A

INNER JOIN gcp-wow-supers-rtla-qtm-dev.TrashPanda.Meat_Seg_Transition_V2 B 
  ON A.CRN = B.CRN

WHERE TIME_PERIOD2 = 'LY'
  AND CATEGORY = 'RED MEAT'
  AND GROUP_POULTRY = 'LIGHT POULTRY'
  AND GROUP_REDMEAT = 'HEAVY RED MEAT'

GROUP BY 1

ORDER BY 2 ASC

