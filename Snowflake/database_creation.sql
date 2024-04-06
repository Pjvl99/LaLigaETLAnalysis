CREATE OR REPLACE DATABASE laliga_statistics;
CREATE OR REPLACE SCHEMA laliga_statistics.laliga;

create or replace storage integration gcp_laliga
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = GCS
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://trim-heaven-415202/laliga_output/hitorical_classification/', 'gcs://trim-heaven-415202/laliga_output/historical_position/',
                                'gcs://trim-heaven-415202/laliga_output/results/', 'gcs://trim-heaven-415202/laliga_output/scorers/');


USE laliga_statistics;

create or replace file format laliga_statistics.public.fileformat_laliga
  TYPE = PARQUET;

CREATE OR REPLACE stage laliga_statistics.public.stage_historical_classification
  STORAGE_INTEGRATION = gcp_laliga
  URL = 'gcs://trim-heaven-415202/laliga_output/hitorical_classification/'
  FILE_FORMAT = fileformat_laliga;

CREATE OR REPLACE stage laliga_statistics.public.stage_historical_position
  STORAGE_INTEGRATION = gcp_laliga
  URL = 'gcs://trim-heaven-415202/laliga_output/historical_position/'
  FILE_FORMAT = fileformat_laliga;

CREATE OR REPLACE stage laliga_statistics.public.stage_results
  STORAGE_INTEGRATION = gcp_laliga
  URL = 'gcs://trim-heaven-415202/laliga_output/results/'
  FILE_FORMAT = fileformat_laliga;

CREATE OR REPLACE stage laliga_statistics.public.stage_scorers
  STORAGE_INTEGRATION = gcp_laliga
  URL = 'gcs://trim-heaven-415202/laliga_output/scorers/'
  FILE_FORMAT = fileformat_laliga;