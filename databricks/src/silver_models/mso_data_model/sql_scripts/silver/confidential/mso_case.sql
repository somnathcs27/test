SELECT
  CAST(UPPER(c.CaseId) AS STRING) AS CASE_ID,
  CAST(c.FriendlyId AS STRING) AS FRIENDLY_ID,
  -- Knocks the prefix letter off of the FriendlyId
  CAST(
    RIGHT(
      c.FriendlyId,
      LENGTH(c.FriendlyId) - 1) 
    AS BIGINT
  ) AS FRIENDLY_ID_NUMBER,
  /*
    The TRY_CAST on MortgageAccountNumber here is for Dev/SIT only where we have old test data with 
    dodgy account numbers, we've been assured that these instances DO NOT exist in production.
    When reconciling in Dev and SIT, for any cases where MSO has an account number but our 
    silver table does not, it is probably one of these cases. For reconciliation, we can join
    back to bronze on CaseId & FriendlyId and grab the original MortgageAccountNumber.

    We won't be made aware of any account number problems in Prod because of the TRY_CAST, but
    CAST'ing MortgageAccountNumber to a STRING would have the same effect, so any issues will need
    to be smoked out by Analysts during reconciliation of their reports/PBI models.
  */
  TRY_CAST(c.MortgageAccountNumber AS BIGINT) AS MORTGAGE_ACCOUNT_NUMBER,
  CAST(c.CreatedDate AS TIMESTAMP) AS CREATED_DATETIME,
  CAST(c.SubmissionDate AS TIMESTAMP) AS SUBMISSION_DATETIME,
  CAST(c.UploadDate AS TIMESTAMP) AS UPLOAD_DATETIME,
  CAST(c.LastUpdatedDate AS TIMESTAMP) AS LAST_UPDATED_DATETIME,
  CAST(c.Channel AS STRING) AS CHANNEL,
  CAST(c.IntroductoryChannel AS STRING) AS INTRODUCTORY_CHANNEL,
  CAST(c.CaseStage AS STRING) AS CASE_STAGE,
  CAST(c.LatestCaseStatus AS STRING) AS LATEST_CASE_STATUS,
  CAST(c.ReasonForStatus AS STRING) AS REASON_FOR_STATUS,
  CAST(c.AmendRecommendationReason AS STRING) AS AMEND_RECOMMENDATION_REASON,
  CAST(c.Brand AS STRING) AS BRAND,
  CAST(c.SourcePortal AS STRING) AS SOURCE_PORTAL,
  CAST(UPPER(c.CreatedByUserId) AS STRING) AS CREATED_BY_USER_ID,
  CAST(UPPER(c.CreatedByUserName) AS STRING) AS CREATED_BY_USERNAME,
  CAST(UPPER(c.CreatedByForename) AS STRING) AS CREATED_BY_FORENAME,
  CAST(UPPER(c.CreatedBySurname) AS STRING) AS CREATED_BY_SURNAME,
  CAST(UPPER(c.IntroducingTeamId) AS INT) AS INTRODUCING_TEAM_ID,
  CAST(UPPER(c.IntroducingTeam) AS STRING) AS INTRODUCING_TEAM,
  CAST(c.MarketingCode AS STRING) AS MARKETING_CODE,
  CAST(c.MarketingSource AS STRING) AS MARKETING_SOURCE,
  CAST(UPPER(c.OwnerUserName) AS STRING) AS OWNER_USER_NAME,
  CAST(UPPER(c.OwnerForename) AS STRING) AS OWNER_FORENAME,
  CAST(UPPER(c.OwnerSurname) AS STRING) AS OWNER_SURNAME,
  CAST(UPPER(c.OwnerTeam) AS STRING) AS OWNER_TEAM,
  CAST(c.ApplicationRegulation AS STRING) AS APPLICATION_REGULATION,
  CAST(c.CapitalisedFees AS DECIMAL(8, 2)) AS CAPITALISED_FEES,
  CAST(c.HasRegulatedFlag AS TINYINT) AS HAS_REGULATED_FLAG,
  CAST(c.ConveyancingNotRequired AS TINYINT) AS CONVEYANCING_NOT_REQUIRED,
  CAST(c.FCARegulated AS TINYINT) AS FCA_REGULATED,
  CAST(c.MDP_ROW_HASH AS STRING) AS MDP_ROW_HASH,
  CAST(c.MDP_LOAD_ID AS BIGINT) AS MDP_LOAD_ID,
  CAST(c.MDP_LOAD_DATETIME AS TIMESTAMP) AS MDP_LOAD_DATETIME,
  CAST(c.__START_AT AS TIMESTAMP) AS ROW_START_DATETIME,
  CAST(c.__END_AT AS TIMESTAMP) AS ROW_END_DATETIME
FROM {env_var}_catalog.bronze_mso_con.case c
WHERE __END_AT IS NULL -- Equivalent of ROW_IS_CURRENT = 1