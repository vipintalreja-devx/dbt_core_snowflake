-- =============================================================================
-- 12_native_seam_test.sql -- read access for the dbt <-> native seam test
-- (tests/singular/assert_native_n04_matches_cdr_voice.sql). Read-only, one table.
-- =============================================================================
USE ROLE SYSADMIN;
USE DATABASE TELCO_PROD_XL_V2;
GRANT USAGE ON SCHEMA NATIVE_MART TO ROLE DBT_ROLE;
GRANT SELECT ON TABLE NATIVE_MART.N04_VOICE_INBOX TO ROLE DBT_ROLE;
