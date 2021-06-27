# ETL Pipelines for the SRM project

The ETL System we use is based on the DGP-APP Platform

## Important files and directories

- `configuration.json` - the main DGP app configuration file
- `srm_tools/` - a utility python package, for common code and tools not specific for a single operator
- `events/` - DGP Event handlers (TBD)
- `operators/` - The specific pipeline operators code

## Configuration

### Environment variables:

**Authentication**
- `EXTERNAL_ADDRESS`: External address of the website (used to set auth callback correctly)
- `GOOGLE_KEY`: Credentials key for oauth2 authentication
- `GOOGLE_SECRET`: Credentials secret for oauth2 authentication
- `DGP_APP_DEFAULT_ROLE`: Set to `1`, disallowing any anonymous access
- `PUBLIC_KEY` & `PRIVATE_KEY`: PEM encoded RSA key pair, used to encode JWT for the client

**Source file storage**:
- `BUCKET_NAME`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `S3_ENDPOINT_URL`: The usual meaning

**Databases**:
- `DATABASE_URL`: Connection string for the `auth` database
- `DATASETS_DATABASE_URL`: Connection string for the `datasets` database
- `ETLS_DATABASE_URL`: Connection string for the `etls` database
- `AIRFLOW__CORE__SQL_ALCHEMY_CONN`: Connection string for the `airflow` database

**Scraper Specific**:

- `GUIDESTAR_USERNAME` - username for logging into Guidestar api
- `GUIDESTAR_PASSWORD` - password for logging into Guidestar api

- `DATAFLOWS_AIRTABLE_APIKEY` - API key for an airtable user allowed to access AirTable bases