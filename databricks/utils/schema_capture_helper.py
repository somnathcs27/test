# Databricks notebook source
# MAGIC %md
# MAGIC # `schema_capture_helper`
# MAGIC
# MAGIC A helper script which attempts to spit out a block of metadata rows for insertion into `populate_control_task_target_table_schema.sql` in the `MDP_Workflow_Control` database project.
# MAGIC
# MAGIC The original use-case for this script was to capture the inferred schema for the LPS lending products source (originally a database view), prior to us moving to the more reliable and stable load of text files from the `\\lbs-win-bi` file server. We wanted to ensure our new data load produced tables with the same _exact_ schema as for the view, building confidence in our downstream table operations working on the basis of a drop-in replacement of sources in silver.
# MAGIC
# MAGIC ⚠️ READ ALL OUTPUTS BEFORE PASTING INTO METADATA ⚠️
# MAGIC
# MAGIC _this is a tool, not established process_

# COMMAND ----------

params = dbutils.widgets.getAll()

df = spark.sql(f"SELECT * FROM {params.get('CATALOG')}.{params.get('SCHEMA')}.{params.get('TABLE')}")

columns, manual_timestamp_count = [], 0

for field in df.schema:

  field_str = field.simpleString()

  # Pass on MDP metadata cols.
  if field_str.startswith("MDP_"):
    continue

  field_parts = field_str.split(':')
  column_name, column_type = [f.upper() for f in field_parts]
  if len(field_parts) != 2:
    raise ValueError("Unexpected return on simpleString - we expect one column!")

  precision, scale = 'NULL', 'NULL'
  # check if data type has extent or scale/precision.
  if column_type.startswith('DECIMAL'):
    checks = ['(' in column_type, ')' in column_type]
    if not all(checks):
      raise ValueError("Something unexpected in decimal schema entry... check!")

    # get the expected two items.
    items = column_type[(column_type.index('(') + 1):column_type.index(')')].split(',')
    if not len(items) == 2:
      raise ValueError("Unexpected number of items in decimal schema entry... check!")

    precision, scale = items
    column_type = column_type[0:column_type.index('(')]

  nullable = 1 if field.nullable else 0
  enabled = 1 # we're in the schema.... better be enabled?

  timestamp_format = 'NULL'
  if column_type == 'TIMESTAMP':
    manual_timestamp_count += 1
    timestamp_format = "'<MANUAL TIMESTAMP FORMAT>'"

  colspec_row = [
    f"'{params.get('JOB').upper()}'",
    f"'{params.get('TABLE').upper()}'",
    f"'{column_name}'",
    f"'{column_type}'",
    f'{precision}',
    f'{scale}',
    f'{len(columns) + 1}',
    f'{nullable}',
    f'{enabled}',
    f'{timestamp_format}'
  ]

  columns.append(f"({', '.join(colspec_row)})")

if manual_timestamp_count > 0:
  print(f"BEWARE: there are {manual_timestamp_count} TIMESTAMP fields you need to supply source formatting for!")

preamble = f"""
        INSERT INTO #TaskTargetSchemaTemp
        (
        JobName,
        TaskName,
        ColumnName,
        ColumnDataType,
        ColumnPrecision,
        ColumnScale,
        ColumnOrdinalPosition,
        IsColumnNullable,
        [Enabled],
        SourceDataDatetimeFormat
        )
        VALUES
        /**** {params.get('JOB').upper()} ****/"""

metadata = '\n        ,'.join(columns)

print(f"{preamble}\n        {metadata}")

# COMMAND ----------

