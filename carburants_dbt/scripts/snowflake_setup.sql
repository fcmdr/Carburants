-- ============================================================
-- Snowflake Initial Setup Script for Carburants DBT Project
-- ============================================================
-- Run this script as ACCOUNTADMIN to set up the required
-- databases, warehouses, roles, and users.
-- ============================================================

-- Use accountadmin role for setup
USE ROLE ACCOUNTADMIN;

-- ============================================================
-- 1. Create Warehouses
-- ============================================================

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Main compute warehouse for dbt transformations';

-- ============================================================
-- 2. Create Databases
-- ============================================================

-- Development database
CREATE DATABASE IF NOT EXISTS CARBURANTS_DEV
    COMMENT = 'Development environment for fuel prices project';

-- Production database
CREATE DATABASE IF NOT EXISTS CARBURANTS_PROD
    COMMENT = 'Production environment for fuel prices project';

-- CI database (for pull request builds)
CREATE DATABASE IF NOT EXISTS CARBURANTS_CI
    COMMENT = 'CI environment for pull request testing';

-- ============================================================
-- 3. Create Schemas
-- ============================================================

-- Development schemas
USE DATABASE CARBURANTS_DEV;
CREATE SCHEMA IF NOT EXISTS RAW COMMENT = 'Raw data from ingestion';
CREATE SCHEMA IF NOT EXISTS STAGING COMMENT = 'Staging layer models';
CREATE SCHEMA IF NOT EXISTS INTERMEDIATE COMMENT = 'Intermediate layer models';
CREATE SCHEMA IF NOT EXISTS MARTS COMMENT = 'Marts layer models';
CREATE SCHEMA IF NOT EXISTS SNAPSHOTS COMMENT = 'SCD Type 2 snapshots';
CREATE SCHEMA IF NOT EXISTS SEEDS COMMENT = 'Seed data';

-- Production schemas
USE DATABASE CARBURANTS_PROD;
CREATE SCHEMA IF NOT EXISTS RAW COMMENT = 'Raw data from ingestion';
CREATE SCHEMA IF NOT EXISTS STAGING COMMENT = 'Staging layer models';
CREATE SCHEMA IF NOT EXISTS INTERMEDIATE COMMENT = 'Intermediate layer models';
CREATE SCHEMA IF NOT EXISTS MARTS COMMENT = 'Marts layer models';
CREATE SCHEMA IF NOT EXISTS SNAPSHOTS COMMENT = 'SCD Type 2 snapshots';
CREATE SCHEMA IF NOT EXISTS SEEDS COMMENT = 'Seed data';

-- ============================================================
-- 4. Create Roles
-- ============================================================

-- Transformer role for dbt
CREATE ROLE IF NOT EXISTS TRANSFORMER
    COMMENT = 'Role for dbt transformations';

-- Reader role for BI tools
CREATE ROLE IF NOT EXISTS READER
    COMMENT = 'Read-only role for BI tools and analysts';

-- ============================================================
-- 5. Grant Permissions
-- ============================================================

-- Warehouse permissions
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORMER;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE READER;

-- Database permissions for TRANSFORMER
GRANT ALL ON DATABASE CARBURANTS_DEV TO ROLE TRANSFORMER;
GRANT ALL ON DATABASE CARBURANTS_PROD TO ROLE TRANSFORMER;
GRANT ALL ON DATABASE CARBURANTS_CI TO ROLE TRANSFORMER;

-- Schema permissions for TRANSFORMER
GRANT ALL ON ALL SCHEMAS IN DATABASE CARBURANTS_DEV TO ROLE TRANSFORMER;
GRANT ALL ON ALL SCHEMAS IN DATABASE CARBURANTS_PROD TO ROLE TRANSFORMER;
GRANT ALL ON ALL SCHEMAS IN DATABASE CARBURANTS_CI TO ROLE TRANSFORMER;

-- Future grants for TRANSFORMER
GRANT ALL ON FUTURE SCHEMAS IN DATABASE CARBURANTS_DEV TO ROLE TRANSFORMER;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE CARBURANTS_PROD TO ROLE TRANSFORMER;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE CARBURANTS_CI TO ROLE TRANSFORMER;

GRANT ALL ON FUTURE TABLES IN DATABASE CARBURANTS_DEV TO ROLE TRANSFORMER;
GRANT ALL ON FUTURE TABLES IN DATABASE CARBURANTS_PROD TO ROLE TRANSFORMER;
GRANT ALL ON FUTURE VIEWS IN DATABASE CARBURANTS_DEV TO ROLE TRANSFORMER;
GRANT ALL ON FUTURE VIEWS IN DATABASE CARBURANTS_PROD TO ROLE TRANSFORMER;

-- Read permissions for READER (production only)
GRANT USAGE ON DATABASE CARBURANTS_PROD TO ROLE READER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE CARBURANTS_PROD TO ROLE READER;
GRANT SELECT ON ALL TABLES IN DATABASE CARBURANTS_PROD TO ROLE READER;
GRANT SELECT ON ALL VIEWS IN DATABASE CARBURANTS_PROD TO ROLE READER;

-- Future grants for READER
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE CARBURANTS_PROD TO ROLE READER;
GRANT SELECT ON FUTURE TABLES IN DATABASE CARBURANTS_PROD TO ROLE READER;
GRANT SELECT ON FUTURE VIEWS IN DATABASE CARBURANTS_PROD TO ROLE READER;

-- ============================================================
-- 6. Create Users (customize these!)
-- ============================================================

-- Create dbt service account
CREATE USER IF NOT EXISTS DBT_SERVICE_ACCOUNT
    PASSWORD = 'CHANGE_ME_IMMEDIATELY'  -- Change this!
    DEFAULT_WAREHOUSE = COMPUTE_WH
    DEFAULT_ROLE = TRANSFORMER
    MUST_CHANGE_PASSWORD = TRUE
    COMMENT = 'Service account for dbt transformations';

GRANT ROLE TRANSFORMER TO USER DBT_SERVICE_ACCOUNT;

-- ============================================================
-- 7. Verify Setup
-- ============================================================

SHOW DATABASES LIKE 'CARBURANTS%';
SHOW WAREHOUSES LIKE 'COMPUTE%';
SHOW ROLES LIKE '%TRANSFORMER%';
SHOW ROLES LIKE '%READER%';

-- ============================================================
-- Setup Complete!
-- ============================================================
-- Next steps:
-- 1. Change the DBT_SERVICE_ACCOUNT password
-- 2. Update profiles.yml with your credentials
-- 3. Run: dbt debug
-- ============================================================
