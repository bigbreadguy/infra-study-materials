locals {
  # Temporary migration contract for tables that were already created by the
  # earlier lesson schema. Use this only to flip deletion_protection=false before
  # replacing the tables with local.bigquery_tables_mongo_contract.
  bigquery_tables_legacy_contract = {
    dim_grains = {
      friendly_name   = "Securities grains"
      table_role      = "dimension"
      description     = "Stable dimension of sample grains used by the securities transform-load workflow."
      clustering      = []
      partition_field = null
      schema = [
        {
          name        = "grain_code"
          type        = "STRING"
          mode        = "REQUIRED"
          description = "Stable grain key, for example daily or monthly."
        },
        {
          name        = "grain_name"
          type        = "STRING"
          mode        = "REQUIRED"
          description = "Human-readable grain name."
        },
        {
          name        = "grain_description"
          type        = "STRING"
          mode        = "NULLABLE"
          description = "Optional explanation of the grain."
        },
        {
          name        = "grain_order"
          type        = "INTEGER"
          mode        = "NULLABLE"
          description = "Optional sort order for presentation."
        },
        {
          name        = "is_active"
          type        = "BOOLEAN"
          mode        = "REQUIRED"
          description = "Whether the grain is available for new sample facts."
        },
        {
          name        = "created_at"
          type        = "TIMESTAMP"
          mode        = "REQUIRED"
          description = "Timestamp when the row first entered the table."
        },
        {
          name        = "updated_at"
          type        = "TIMESTAMP"
          mode        = "REQUIRED"
          description = "Timestamp when the row was last upserted."
        },
      ]
    }

    dim_metrics = {
      friendly_name   = "Securities metrics"
      table_role      = "dimension"
      description     = "Stable dimension of sample metrics used by the securities transform-load workflow."
      clustering      = []
      partition_field = null
      schema = [
        {
          name        = "metric_code"
          type        = "STRING"
          mode        = "REQUIRED"
          description = "Stable metric key, for example close_price or volume."
        },
        {
          name        = "metric_name"
          type        = "STRING"
          mode        = "REQUIRED"
          description = "Human-readable metric name."
        },
        {
          name        = "metric_description"
          type        = "STRING"
          mode        = "NULLABLE"
          description = "Optional explanation of the metric."
        },
        {
          name        = "unit"
          type        = "STRING"
          mode        = "NULLABLE"
          description = "Unit of measure for metric_value."
        },
        {
          name        = "value_type"
          type        = "STRING"
          mode        = "REQUIRED"
          description = "Expected value type, such as numeric or percent."
        },
        {
          name        = "metric_order"
          type        = "INTEGER"
          mode        = "NULLABLE"
          description = "Optional sort order for presentation."
        },
        {
          name        = "is_active"
          type        = "BOOLEAN"
          mode        = "REQUIRED"
          description = "Whether the metric is available for new sample facts."
        },
        {
          name        = "created_at"
          type        = "TIMESTAMP"
          mode        = "REQUIRED"
          description = "Timestamp when the row first entered the table."
        },
        {
          name        = "updated_at"
          type        = "TIMESTAMP"
          mode        = "REQUIRED"
          description = "Timestamp when the row was last upserted."
        },
      ]
    }

    fact_values = {
      friendly_name   = "Market index facts"
      table_role      = "fact"
      description     = "Sample market index facts upserted by transform-load SQL and partitioned by sample_date."
      clustering      = ["market_index_code", "metric_code", "grain_code"]
      partition_field = "sample_date"
      schema = [
        {
          name        = "market_index_code"
          type        = "STRING"
          mode        = "REQUIRED"
          description = "Stable market index key from the sample source."
        },
        {
          name        = "sample_date"
          type        = "DATE"
          mode        = "REQUIRED"
          description = "Business date represented by the fact row."
        },
        {
          name        = "grain_code"
          type        = "STRING"
          mode        = "REQUIRED"
          description = "Grain key matching dl_bloomberg_data.dim_grains."
        },
        {
          name        = "metric_code"
          type        = "STRING"
          mode        = "REQUIRED"
          description = "Metric key matching dl_bloomberg_data.dim_metrics."
        },
        {
          name        = "metric_value"
          type        = "NUMERIC"
          mode        = "NULLABLE"
          description = "Measured sample value."
        },
        {
          name        = "currency_code"
          type        = "STRING"
          mode        = "NULLABLE"
          description = "ISO-like currency code when the metric is monetary."
        },
        {
          name        = "source_system"
          type        = "STRING"
          mode        = "REQUIRED"
          description = "Logical source name for lineage; keep real targets out of committed config."
        },
        {
          name        = "source_record_id"
          type        = "STRING"
          mode        = "NULLABLE"
          description = "Optional source-side id used to trace the sample row."
        },
        {
          name        = "run_id"
          type        = "STRING"
          mode        = "REQUIRED"
          description = "Airflow run id or other orchestration id that produced this row."
        },
        {
          name        = "loaded_at"
          type        = "TIMESTAMP"
          mode        = "REQUIRED"
          description = "Timestamp when the sample was loaded or transformed."
        },
        {
          name        = "updated_at"
          type        = "TIMESTAMP"
          mode        = "REQUIRED"
          description = "Timestamp when the row was last upserted."
        },
      ]
    }
  }

  bigquery_tables = var.bigquery_table_schema_contract == "legacy" ? local.bigquery_tables_legacy_contract : local.bigquery_tables_mongo_contract
}
