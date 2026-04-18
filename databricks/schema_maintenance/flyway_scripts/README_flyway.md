# Flyway Documentation

## Intro
The flyway implementation is a tactical piece of work to allow schema migration for managed tables in advance of the PP1 Go-Live, and will be revisited in the context of DDRG.

The flyway implementation has the following basic requirements:
1. One logical SQL statement per script (i.e., no semi-colons please)
2. An initial comment line which defines a check to be passed in order to execute the script.

## Getting Started

### Naming a DDL script
When you want to make a change to a managed table, add a sql script (with the suitable ALTER statement) to this folder (schema_maintenance/flyway_scripts) with the name:

V<number>--<description-of-change><name of table>.sql

The components of this name are:
1. <number> : the ordered number of this change (determines the order in which the flyway scripts run)
2. <description-of-change> : a dash separated description of the change (e.g., add-columnC-to-)
3. <name of table> : the name of the table from unity-catalog

So a full name example would be: V1--add-columnC-to-schema_migrate_test_table1.sql

Commentary:
  - The double dashes after the V<number> separate the description from the version number.
  - The words of the description are separated by dashes (not underscores, because we have used underscores in the table names). If we use dashes in the table name this system will break.
  - The name of the script determines how the schema_history table is populated so its important to get it right.

### Format of the script

An example script is 
```sql
-- {"check_type":"column_not_present", "table_schema":"schema_migration", "table_name":"schema_migrate_test_table1", "column_name":"columnC"}
ALTER TABLE {catalog_name}.schema_migration.schema_migrate_test_table1 ADD COLUMNS (columnC int)
```

Commentary:
  - Note the initial comment with the JSON object defined. This is where the check is defined.
  - This check-comment has to be all on one line. Do not break it up on multiple lines.
  - There are two allowable values for check_type: column_present or column_not_present
  - The idea is that for an ADD column operation we need to check to make sure the column is not already present (or for a DROP column we need to ensure that the column is present)
  - If this check fails, the script will be skipped. This is an additional layer of protection for accidentally running the scripts (e.g., if someone cleared down the mdp_flyway_schema_history table and then re-ran the scripts)
  - There can only be one logical SQL command in the script. So no semi-colons are allowed. This is to prevent the partial-execution scenario where one statement executes, but a subsequent one fails. In this manner, the records added to mdp_flyway_schema_history reflect the isolated SQL commands which have successfully executed.

### Deploy the change

After getting the change committed to the repository (and pull-requested into main), deploy your change by deploying databricks to an environment. The nb_flyway_migration_py.py notebook handles the rollout of the change. 

If the change fails, an entry will be added to the mdp_flyway_schema_failures table, and the notebook will exit without adding a "success row" to the mdp_flyway_schema_history table. In this way, the offending script can be fixed, and retried.

### Script structure

You can provide multiple lines of SQL in the script separated with semicolons, which will run in different spark.sql() commands.

## Flyway Logic

The basic idea behind the initial flyway implementation is that each script should only be successfully attempted once. 

Successfully attempted is defined by:
  1. Pass the initial check & execute the SQL script succesfully
  2. Fail the initial check and skip the SQL script

The only case where the script will be re-tried is where the SQL execution (or some other element of the flyway python notebook) throws an exception during the processing of a given script. This interrupts the whole flyway process, and adds a row to the mdp_flyway_schema_failures table. When the script is fixed - the deployment can be retried, and flyway will pick up where it left off (and correctly retry the script).

The case of failing the initial check and skipping the SQL script will add a row to the mdp_flyway_schema_history table, and will therefore block a retry of this script in future. If a retry is necessary, an additional flyway script will be required.

The rationale behind this "fail check, log success" logic is the following:

  - There may well be legitimate reasons why a script should not execute:
    - For example, for our add-column scripts we are keeping the initial create scripts updated, 
    - Therefore if we deploy to a brand-new environment like pre-prod the initial create will contain all of the added columns from the flyway scripts
    - After these scripts have been attempted (the initial check will fail), they should not be attempted again.
  - If the "fail check" (e.g., column A present for a script to add column A) did not result in populating the mdp_flyway_schema_history table, then this script would be attempted every time, and would always fail the check. It would never be marked as successfully attempted.
  - It is believed that this approach will keep the environments aligned in a more ordered and predictable manner than if we leave the "fail initial check" cases to retry for ever.
  - However - we really need to gain a few months experience with the tactical solution before evolving it any further.