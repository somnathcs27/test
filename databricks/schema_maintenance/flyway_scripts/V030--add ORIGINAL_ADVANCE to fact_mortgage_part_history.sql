-- {"check_type":"column_not_present", "table_schema":"gold_con", "table_name":"fact_mortgage_part_history", "column_name":"ORIGINAL_ADVANCE"}
ALTER TABLE {catalog_name}.gold_con.fact_mortgage_part_history
ADD COLUMN ORIGINAL_ADVANCE DECIMAL(38,2) AFTER REMAINING_MONTHS;
