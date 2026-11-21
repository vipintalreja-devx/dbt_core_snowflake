-- =============================================================================
-- 06_lineage.sql -- lineage a migration/catalog tool can read without dbt.
--   OPS.LINEAGE_EDGES        declared edges (dbt graph, republished every run)
--   OPS.V_ACCESS_LINEAGE     observed edges: ACCESS_HISTORY read -> write per dbt query
--   OPS.V_LINEAGE_RECONCILE  declared vs observed, flags edges seen on one side only
--   OPS.V_SOURCE_TO_MEDALLION  bronze schema -> medallion schema summary
-- Owner: DBT_ROLE (has IMPORTED PRIVILEGES on the SNOWFLAKE database).
-- ACCOUNT_USAGE latency is up to ~3 h, so fresh runs appear in the observed view later.
-- =============================================================================
USE ROLE DBT_ROLE;
USE DATABASE TELCO_PROD_XL_V2;

CREATE OR REPLACE VIEW OPS.V_ACCESS_LINEAGE
  COMMENT = 'Observed table-level lineage of dbt queries (ACCESS_HISTORY base objects read -> objects written)'
AS
WITH q AS (
    SELECT query_id
      FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
     WHERE start_time > DATEADD('day', -30, CURRENT_TIMESTAMP())
       AND query_tag = 'dbt_telco'
       AND database_name = 'TELCO_PROD_XL_V2'
), ah AS (
    SELECT a.query_id, a.query_start_time, a.base_objects_accessed, a.objects_modified
      FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY a
      JOIN q ON q.query_id = a.query_id
     WHERE a.query_start_time > DATEADD('day', -30, CURRENT_TIMESTAMP())
       AND ARRAY_SIZE(a.objects_modified) > 0
)
SELECT UPPER(src.value:"objectName"::STRING)   AS upstream_object,
       src.value:"objectDomain"::STRING        AS upstream_domain,
       UPPER(tgt.value:"objectName"::STRING)   AS downstream_object,
       tgt.value:"objectDomain"::STRING        AS downstream_domain,
       COUNT(DISTINCT ah.query_id)             AS queries,
       MAX(ah.query_start_time)                AS last_seen_at
  FROM ah,
       LATERAL FLATTEN(input => ah.base_objects_accessed) src,
       LATERAL FLATTEN(input => ah.objects_modified) tgt
 WHERE UPPER(src.value:"objectName"::STRING) <> UPPER(tgt.value:"objectName"::STRING)
   AND UPPER(src.value:"objectName"::STRING) LIKE 'TELCO_PROD_XL_V2.%'
   AND UPPER(src.value:"objectName"::STRING) NOT LIKE 'TELCO_PROD_XL_V2.OPS.%'
   AND UPPER(tgt.value:"objectName"::STRING) NOT LIKE 'TELCO_PROD_XL_V2.OPS.%'
   AND UPPER(tgt.value:"objectName"::STRING) NOT LIKE '%_DBT_TMP'
 GROUP BY 1, 2, 3, 4;

CREATE OR REPLACE VIEW OPS.V_LINEAGE_RECONCILE
  COMMENT = 'Declared (dbt) vs observed (ACCESS_HISTORY) lineage edges'
AS
SELECT COALESCE(d.upstream_object, o.upstream_object)     AS upstream_object,
       COALESCE(d.downstream_object, o.downstream_object) AS downstream_object,
       d.upstream_layer, d.downstream_layer, d.materialization,
       d.upstream_object IS NOT NULL                      AS is_declared,
       o.upstream_object IS NOT NULL                      AS is_observed,
       o.last_seen_at,
       CASE WHEN d.upstream_object IS NOT NULL AND o.upstream_object IS NOT NULL THEN 'MATCHED'
            WHEN d.upstream_object IS NOT NULL THEN 'DECLARED_ONLY'   -- e.g. not run in the last 30 days, or ACCESS_HISTORY latency
            ELSE 'OBSERVED_ONLY' END                       AS status
  FROM OPS.LINEAGE_EDGES d
  FULL OUTER JOIN OPS.V_ACCESS_LINEAGE o
    ON o.upstream_object = d.upstream_object AND o.downstream_object = d.downstream_object;

CREATE OR REPLACE VIEW OPS.V_SOURCE_TO_MEDALLION
  COMMENT = 'Which bronze schemas feed which medallion schemas (declared lineage)'
AS
SELECT SPLIT_PART(upstream_object, '.', 2)   AS upstream_schema,
       SPLIT_PART(downstream_object, '.', 2) AS downstream_schema,
       COUNT(*)                              AS edges,
       COUNT(DISTINCT upstream_object)       AS upstream_objects,
       COUNT(DISTINCT downstream_object)     AS downstream_objects
  FROM OPS.LINEAGE_EDGES
 GROUP BY 1, 2;
