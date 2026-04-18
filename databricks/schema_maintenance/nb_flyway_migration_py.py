# Databricks notebook source
# MAGIC %md
# MAGIC # Flyway Schema Migration for Delta Tables

# COMMAND ----------

dbutils.widgets.text(name='catalog', defaultValue='dev_catalog')
catalog_name = dbutils.widgets.get("catalog")

dbutils.widgets.text(name='env_var', defaultValue = 'dev')
env_var = dbutils.widgets.get("env_var")

dbutils.widgets.text(name='common_libs_lts_ver', defaultValue = '')
common_libs_lts_ver = dbutils.widgets.get("common_libs_lts_ver")

dbutils.widgets.text(name='common_libs_lts_ver_underscore', defaultValue = '')
common_libs_lts_ver_underscore = dbutils.widgets.get("common_libs_lts_ver_underscore")

mdp_databricks_common_library = f"/Volumes/{env_var}_catalog/common_libraries/mdp_databricks_common_{common_libs_lts_ver_underscore}/mdp_databricks_common-{common_libs_lts_ver}-py3-none-any.whl"

mdp_databricks_common_library_wheelhouse = f"--no-index --find-links '/Volumes/{env_var}_catalog/common_libraries/wheelhouse/' '/Volumes/{env_var}_catalog/common_libraries/mdp_databricks_common_{common_libs_lts_ver_underscore}/mdp_databricks_common-{common_libs_lts_ver}-py3-none-any.whl'"

# COMMAND ----------

# MAGIC %pip uninstall $mdp_databricks_common_library -y 
# MAGIC %pip install $mdp_databricks_common_library_wheelhouse

# COMMAND ----------

from mdp_databricks_common.feature_toggle import feature_toggle as ft

# COMMAND ----------

from dataclasses import dataclass
from typing import Set, List
from datetime import datetime
from pyspark.sql import SparkSession
from typing import Any
import json

@dataclass
class FlywaySchemaMigrator:
    spark: SparkSession
    dbutils: Any
    catalog_name: str
    uc_volumes_schema_migration_root: str
    schema_history_table: str
    schema_failures_table: str


    def get_migration_files(self) -> List[str]:
        files_info = self.dbutils.fs.ls(self.uc_volumes_schema_migration_root)

        migration_files = [f.name for f in files_info if f.name.endswith(".sql") and f.name.startswith("V")]

        def version_key(filename):
            v = filename.split("--")[0][1:]
            return tuple(int(x) for x in v.split("."))
        
        return sorted(migration_files, key=version_key)


    def get_applied_migrations(self) -> Set[str]:
        try:
            df = self.spark.table(self.schema_history_table).select("script").collect()
            return set(row.script for row in df)
        except Exception as e:
            raise Exception(f"Error reading schema history table in flyway get_applied_migrations. Original error: {e}") from e


    def log_failure(self, script_name: str, error_message: str):
        now = datetime.now()
        failure_df = self.spark.createDataFrame(
            [(script_name, now, error_message)],
            "script STRING, failed_on TIMESTAMP, error_message STRING"
        )

        failure_df.write.mode("append").saveAsTable(self.schema_failures_table)
        print(f"Logged failure for {script_name} (see {self.schema_failures_table} for more info)")


    def parse_and_return_required_check(self, script_string: str) -> dict:
        first_line = script_string.strip().splitlines()[0].strip()

        if not first_line.startswith("--"):
            raise ValueError("First line must be a comment (starting with -- ) containing the check JSON object (see README_flyway.md)")
        
        # Strip off the leading "--" we just checked for.
        json_part = first_line[2:].strip()

        try:
            check_data = json.loads(json_part)
        except json.JSONDecodeError:
            raise ValueError("The check JSON object is not formed correctly. Look at the first line of your script more carefully (see README_flyway.md)")

        required_keys = {"check_type", "table_schema", "table_name", "column_name"}
        if not required_keys.issubset(check_data):
            raise ValueError(f"The check JSON object is missing one or more of the required keys: {required_keys} (see README_flyway.md)")
        
        valid_check_types = {"column_present", "column_not_present"}
        if check_data["check_type"] not in valid_check_types:
            raise ValueError(f"Invalid check_type. Must be one of: {valid_check_types} (see README_flyway.md)")
        
        return check_data


    def check_column_existence(self, check_data: dict) -> bool:
        info_schema = f"{self.catalog_name}.information_schema.columns"

        query = f"""
        SELECT 1
        FROM {info_schema}
        WHERE table_schema = '{check_data["table_schema"]}'
           AND table_name = '{check_data["table_name"]}'
           AND column_name = '{check_data["column_name"]}'
        """

        result = self.spark.sql(query)
        return result.count() > 0


    def check_passes(self, script_string_resolved) -> bool:
        check_result = True
        
        check_data = self.parse_and_return_required_check(script_string_resolved)
        column_exists = self.check_column_existence(check_data)

        if check_data["check_type"] == "column_present" and not column_exists:
            check_result = False
            print("Check failed: Column is expected to be present but was not found. Skipping this script.")
        elif check_data["check_type"] == "column_not_present" and column_exists:
            check_result = False
            print("Check failed: Column is expected to be absent but was found. Skipping this script.")

        return check_result


    def execute_script(self, script_string_resolved):
        script_lines = script_string_resolved.strip().splitlines()
        body_sql = "\n".join(script_lines[1:]).strip()
        
        if body_sql:
            # This is the business line where the script actually gets executed.
            self.spark.sql(body_sql)
            print("SQL script executed successfully")
        else:
            print("No SQL body to execute (nothing done)")


    def record_script_success(self, version, description, script_name, timestamp):
        success_df = self.spark.createDataFrame(
            [(version, description, script_name, timestamp)],
            "version STRING, description STRING, script STRING, installed_on TIMESTAMP"
        )

        success_df.write.mode("append").saveAsTable(self.schema_history_table)
        print(f"Migration {version} applied successfully")


    def run_migration(self, script_name: str, version: str, description: str, catalog_name: str):
        full_path = self.uc_volumes_schema_migration_root.rstrip("/") + "/" + script_name
        print(f"Running migration {version}: {description} from {full_path}")

        with open(full_path, "r") as f:
            script_string_content = f.read()

        # Substitute in the actual catalog_name (dev_catalog/sit_catalog/prod_catalog) to the 
        # templated {catalog_name} parameter (requires the use of {catalog_name} in the script)
        script_string_resolved = script_string_content.replace("{catalog_name}", catalog_name)
        print(f"Full script content with catalog substituted in: {script_string_resolved}")

        # Skip the script if the check doesn't pass (e.g., if we're trying to add a column which already exists)
        check_passes_successfully = self.check_passes(script_string_resolved)
        if check_passes_successfully:
            print("check passes. Executing script.")
            self.execute_script(script_string_resolved)
        else:
            print("check failed. Skipping script - but logging the attempt in the schema-history table.")
            
        print("Logging script attempt in schema-history table")            
        self.record_script_success(
            version=version, 
            description=description, 
            script_name=script_name, 
            timestamp=datetime.now()
        )


    def migrate(self):
        # Make sure schema history and failures exist 

        applied = self.get_applied_migrations()
        migrations = self.get_migration_files()

        for script_name in migrations:
            if script_name in applied:
                print(f"Skipping already applied migration: {script_name}")
                continue

            parts = script_name.split("--")
            version = parts[0][1:]
            description = parts[1].replace("-", " ").replace(".sql", "")

            try:
                self.run_migration(script_name, version, description, self.catalog_name)

            except Exception as e:
                error_message = str(e)
                print(f"Migration {script_name} failed with error: {error_message}")
                self.log_failure(script_name, error_message)
                raise e


# COMMAND ----------

migration_schema = "schema_migration"
uc_volumes_schema_migration_root = f"/Volumes/{catalog_name}/{migration_schema}/flyway_schema_migration_scripts"
schema_history_table = f"{catalog_name}.{migration_schema}.mdp_flyway_schema_history"
schema_failures_table = f"{catalog_name}.{migration_schema}.mdp_flyway_schema_failures"

migrator = FlywaySchemaMigrator(
    spark = spark, 
    dbutils = dbutils, 
    catalog_name = catalog_name, 
    uc_volumes_schema_migration_root = uc_volumes_schema_migration_root, 
    schema_history_table = schema_history_table, 
    schema_failures_table = schema_failures_table
)

migrator.migrate()
