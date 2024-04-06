USE laliga_statistics;

CREATE OR REPLACE TABLE historical_classification (
  position integer,
  pts integer,
  pl integer,
  w integer,
  d integer,
  l integer,
  f integer,
  a integer,
  team varchar,
  season varchar
);

CREATE OR REPLACE TABLE scorers (
  player varchar,
  goals integer,
  season varchar,
  real_position integer
);

CREATE OR REPLACE TABLE historical_positions (
  team varchar,
  fixture integer,
  position integer,
  season varchar
);

CREATE OR REPLACE TABLE results (
  local varchar,
  away varchar,
  result varchar,
  local_goals integer,
  away_goals integer,
  season varchar
);