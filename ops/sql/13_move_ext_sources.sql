-- =============================================================================
-- 13_move_ext_sources.sql -- give the last two DBT_V2_RAW tables dbt uses a real source home.
--   DBT_V2_RAW.NETWORK_IP_POOL     -> NETWORK.IP_POOL          (zero-copy clone)
--   DBT_V2_RAW.REF_UNIT_CONVERSION -> REF.UNIT_CONVERSION      (zero-copy clone)
-- dbt sources now read NETWORK.IP_POOL / REF.UNIT_CONVERSION; DBT_V2_RAW is no longer a dbt
-- dependency. The originals in DBT_V2_RAW are left untouched (other readers).
-- DBT_ROLE reads them through the existing FUTURE SELECT grants on NETWORK and REF.
-- =============================================================================
USE ROLE SYSADMIN;
USE DATABASE TELCO_PROD_XL_V2;
CREATE TABLE IF NOT EXISTS NETWORK.IP_POOL CLONE DBT_V2_RAW.NETWORK_IP_POOL
  COMMENT = 'Regional IPv4 address pools (moved from the DBT_V2_RAW extract, 2026-11-22). dbt source network.ip_pool';
CREATE TABLE IF NOT EXISTS REF.UNIT_CONVERSION CLONE DBT_V2_RAW.REF_UNIT_CONVERSION
  COMMENT = 'Unit conversion factors (moved from the DBT_V2_RAW extract, 2026-11-22). dbt source ref.unit_conversion';
GRANT SELECT ON TABLE NETWORK.IP_POOL TO ROLE DBT_ROLE;
GRANT SELECT ON TABLE REF.UNIT_CONVERSION TO ROLE DBT_ROLE;
