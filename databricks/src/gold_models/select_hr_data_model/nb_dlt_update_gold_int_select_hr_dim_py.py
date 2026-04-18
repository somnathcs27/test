# Databricks notebook source
import dlt
from pyspark.sql.functions import expr, row_number, monotonically_increasing_id
from pyspark.sql.window import Window
 
import mdp_databricks_common.gold_layer_functions as dlf
import mdp_databricks_common.silver_layer_functions as dlfs

# Get environment variable from AKV
env_var = dbutils.secrets.get(scope = 'data-kv-scope', key = 'data-platform-environment')

# COMMAND ----------

@dlt.table(
    name="dim_staff",
    comment="Staff Details Dimension"
)
def gold_dim_staff():
    # Execute SQL query to select and transform data
    query = f"""
    WITH CTE_STAFF_UNION AS
    (
        SELECT
        CAST(x.BK_STAFF AS BIGINT) AS BK_STAFF,
        x.BRANCH_CODE,
        x.COLLEAGUE_FIRST_NAME,
        x.COLLEAGUE_LAST_NAME,
        x.COLLEAGUE_FULL_NAME,
        x.JOB_TITLE,
        x.DEPARTMENT,
        x.TELEPHONE,
        x.EMAIL,
        x.STATUS,
        x.COLLEAGUE_START_DATE,
        x.COLLEAGUE_LEAVE_DATE,
        x.ROW_START_DATE,
        x.ROW_END_DATE,
        ROW_NUMBER() OVER(PARTITION BY x.BK_STAFF, x.DEPARTMENT, x.JOB_TITLE ORDER BY x.ROW_START_DATE DESC) AS ROW_NUM
        FROM(
            SELECT
            ea.PAYROLL_NUMBER AS BK_STAFF,
            CAST(occ.COST_TO_NUMBER AS STRING) AS BRANCH_CODE,
            ed.FIRST_NAME AS COLLEAGUE_FIRST_NAME,
            ed.SURNAME AS COLLEAGUE_LAST_NAME,
            ed.FORMAL_NAME AS COLLEAGUE_FULL_NAME,
            opd.POST_NAME AS JOB_TITLE,
            od.UNIT_NAME AS DEPARTMENT,
            ol.PHONE_NUMBER AS TELEPHONE,
            ed.EMAIL,
            ec.STATUS,
            ea.STARTING_DATE AS COLLEAGUE_START_DATE,
            ea.LEAVING_DATE AS COLLEAGUE_LEAVE_DATE,
            ec.EFFECTIVE_DATE AS ROW_START_DATE,
            LEAD(ec.EFFECTIVE_DATE, 1) OVER(PARTITION BY ea.PAYROLL_NUMBER ORDER BY ec.EFFECTIVE_DATE ASC) AS ROW_END_DATE
            from {env_var}_catalog.silver_sec.select_hr_employee_appointment ea
            LEFT JOIN {env_var}_catalog.silver_sec.select_hr_employee_career ec
            ON ea.APPOINTMENT_NUMBER = ec.APPOINTMENT_NUMBER
            LEFT JOIN {env_var}_catalog.silver_sec.select_hr_organisation_location ol
            ON ec.LOCATION_NUMBER = ol.LOCATION_NUMBER
            LEFT JOIN {env_var}_catalog.silver_sec.select_hr_employee_snapshot_data esd
            ON ec.APPOINTMENT_NUMBER = esd.APPOINTMENT_NUMBER
            LEFT JOIN {env_var}_catalog.silver_sec.select_hr_employee_detail ed
            ON esd.PERSON_NUMBER = ed.PERSON_NUMBER
            LEFT JOIN {env_var}_catalog.silver_sec.select_hr_post_detail opd
            ON ec.POST_NUMBER = opd.POST_NUMBER
            LEFT JOIN {env_var}_catalog.silver_sec.select_hr_organisation_key_level okl
            ON ec.POST_NUMBER = okl.POST_NUMBER
            LEFT JOIN {env_var}_catalog.silver_sec.select_hr_organisation_detail od
            ON okl.KEY_UNIT_1 = od.UNIT_NUMBER
            LEFT JOIN {env_var}_catalog.silver_sec.select_hr_cost_centre_detail occ
            ON od.COST_TO_NUMBER = occ.COST_TO_NUMBER
            
            UNION ALL

            SELECT
            scd.COLLEAGUE_ID,
            CAST(SD.COSTCENTRE AS STRING) AS BRANCH_CODE,
            sc.FORENAME AS COLLEAGUE_FIRST_NAME,
            sc.SURNAME AS COLLEAGUE_LAST_NAME,
            scd.COLLEAGUE_NAME,
            scd.JOB_TITLE,
            scd.DEPARTMENT,
            sc.TELEPHONE AS TELEPHONE,
            sc.EMAIL AS EMAIL,
            CAST(sc.STATUS AS STRING) AS STATUS,
            scd.COLLEAGUE_START_DATE,
            scd.COLLEAGUE_LEAVE_DATE,
            scd.SCD_START_DATE,
            scd.SCD_END_DATE
            FROM {env_var}_catalog.silver_con.sap_transform_staff_details_scd scd
            LEFT JOIN {env_var}_catalog.silver_con.sap_staging_sunrise_contact sc
            ON CAST(scd.COLLEAGUE_ID AS INT) = CAST(sc.STAFF_ID AS INT)
            LEFT JOIN {env_var}_catalog.silver_con.sap_staging_sunrise_department sd
            ON sc.DEPARTMENT_ID = sd.DEPARTMENT_ID
        ) x
    ),

    CTE_MANAGER_DETAILS AS
    (
    SELECT DISTINCT
    A.Post_Number AS PK_POST_NUMBER,
    C.Post_Number AS MANAGER_POST_NUMBER,
    C.Post_Name AS MANAGER_POST_NAME,
    C.Post_Unique_Name AS MANAGER_POST_UNIQUE_NAME,
    D.Appointment_Number AS MANAGER_APPOINTMENT_NUMBER,
    F.Person_Number AS MANAGER_PERSON_NUMBER,
    F.Formal_Name AS MANAGER_FORMAL_NAME,
    F.Informal_Name AS MANAGER_INFORMAL_NAME,
    E.Payroll_Number AS MANAGER_ID,
    CASE WHEN G.Manager_Post_Number IS NOT NULL THEN 1 ELSE 0 END AS IS_LEADER,
    D.PAYROLL_NUMBER
    FROM {env_var}_catalog.silver_sec.select_hr_employee_career A
    LEFT JOIN {env_var}_catalog.silver_sec.select_hr_post_detail B
    ON A.Post_Number = B.Post_Number
    LEFT JOIN {env_var}_catalog.silver_sec.select_hr_post_detail C
    ON B.Manager_Post_Number = C.Post_Number
    LEFT JOIN {env_var}_catalog.silver_sec.select_hr_employee_appointment D
    ON A.APPOINTMENT_NUMBER = D.APPOINTMENT_NUMBER
    LEFT JOIN (
    SELECT DISTINCT Post_Number, Appointment_Number,
    CASE WHEN (ROW_NUMBER() OVER( PARTITION BY Post_Number order by Effective_Date  desc)) = 1 THEN 1 ELSE 0 END AS CURRENT_FLAG
    FROM {env_var}_catalog.silver_sec.select_hr_employee_career) H
    ON C.Post_Number = H.Post_Number AND H.CURRENT_FLAG = 1
    LEFT JOIN {env_var}_catalog.silver_sec.select_hr_employee_appointment E
    ON H.Appointment_Number = E.Appointment_Number
    LEFT JOIN {env_var}_catalog.silver_sec.select_hr_employee_snapshot_data I
    ON E.APPOINTMENT_NUMBER = I.APPOINTMENT_NUMBER
    LEFT JOIN {env_var}_catalog.silver_sec.select_hr_employee_detail F
    ON I.Person_Number = F.Person_Number
    LEFT JOIN (
    SELECT DISTINCT Manager_Post_Number FROM {env_var}_catalog.silver_sec.select_hr_post_detail) G
    ON A.Post_Number = G.Manager_Post_Number
    WHERE A.Post_Number IS NOT NULL
    )

    SELECT
    x.BK_STAFF AS BK_STAFF,
    IFNULL(x.BRANCH_CODE, '-1') AS BRANCH_CODE,
    IFNULL(x.DEPARTMENT, 'Unknown') AS BRANCH_DEPARTMENT,
    IFNULL(x.COLLEAGUE_FIRST_NAME, 'Unknown') AS FORENAME,
    IFNULL(x.COLLEAGUE_LAST_NAME, 'Unknown') AS SURNAME,
    IFNULL(x.COLLEAGUE_FULL_NAME, 'Unknown') AS FULL_NAME,
    IFNULL(x.JOB_TITLE, 'Unknown') AS JOB_TITLE,
    IFNULL(x.STATUS, '-1') AS STATUS,
    IFNULL(x.TELEPHONE, '-1') AS TELEPHONE,
    CAST(IFNULL(x.COLLEAGUE_START_DATE,'1900-01-01') AS DATE) AS START_DATE,
    CAST(IFNULL(x.COLLEAGUE_LEAVE_DATE,'1900-01-01') AS DATE) AS LEAVE_DATE,
    IFNULL(md.MANAGER_FORMAL_NAME, 'Unknown') AS LINE_MANAGER,
    IFNULL(x.EMAIL, 'Unknown') AS EMAIL,
    'SELECTHR' AS SOURCE_SYSTEM,
    CAST(IFNULL(x.ROW_START_DATE,'1900-01-01') AS DATE) AS ROW_START_DATE,
    CAST(IFNULL(CASE 
        WHEN x.ROW_END_DATE = '9000-12-31' AND ROW_NUMBER() OVER(PARTITION BY x.BK_STAFF ORDER BY x.ROW_START_DATE DESC) != 1 THEN LAG(x.ROW_START_DATE,1) OVER(PARTITION BY x.BK_STAFF ORDER BY x.ROW_START_DATE DESC)
        ELSE x.ROW_END_DATE
    END,'1900-01-01') AS DATE) AS ROW_END_DATE
    FROM(
        SELECT
        BK_STAFF,
        BRANCH_CODE,
        COLLEAGUE_FIRST_NAME,
        COLLEAGUE_LAST_NAME,
        COLLEAGUE_FULL_NAME,
        JOB_TITLE,
        DEPARTMENT,
        TELEPHONE,
        EMAIL,
        STATUS,
        COLLEAGUE_START_DATE AS COLLEAGUE_START_DATE,
        COLLEAGUE_LEAVE_DATE AS COLLEAGUE_LEAVE_DATE,
        ROW_START_DATE AS ROW_START_DATE,
        ROW_END_DATE AS ROW_END_DATE,
        ROW_NUMBER() OVER(PARTITION BY BK_STAFF ORDER BY ROW_START_DATE DESC) AS ROW_NUM
        FROM CTE_STAFF_UNION
        WHERE ROW_NUM = 1
        ORDER BY ROW_START_DATE DESC
    ) x
    LEFT JOIN CTE_MANAGER_DETAILS md
    ON CAST(x.BK_STAFF AS BIGINT) = md.PAYROLL_NUMBER
    """

    df = spark.sql(query)

    # Add the surrogate key
    df = df.withColumn("PK_STAFF", monotonically_increasing_id() + 1)

    # Define the window specification
    windowSpec = dlfs.get_window_spec("BK_STAFF", "ROW_START_DATE")

    # Add the CURRENT_FLAG column
    df = dlfs.get_row_number(df, windowSpec)

    # Select columns explicitly to make PK_ the first column
    df = df.select(
        "PK_STAFF",
        "BK_STAFF",
        "BRANCH_CODE",
        "BRANCH_DEPARTMENT",
        "FORENAME",
        "SURNAME",
        "FULL_NAME",
        "JOB_TITLE",
        "STATUS",
        "TELEPHONE",
        "START_DATE",
        "LEAVE_DATE",
        "LINE_MANAGER",
        "EMAIL",
        "SOURCE_SYSTEM",
        "ROW_START_DATE",
        "ROW_END_DATE",
        "ROW_IS_CURRENT"
    )

    # Insert dummy values for unknown(-1) and nulls(-2)
    df = dlf.insert_dimension_dummy_rows(df)

    return df
