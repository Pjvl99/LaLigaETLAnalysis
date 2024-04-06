LALIGA_STATISTICS.PUBLIC.HISTORICAL_CLASSIFICATIONUSE laliga_statistics;

COPY INTO historical_classification
  FROM (
    SELECT $1:position::integer,
           $1:pts::integer,
           $1:pl::integer,
           $1:w::integer,
           $1:d::integer,
           $1:l::integer,
           $1:f::integer,
           $1:a::integer,
           $1:team::varchar,
           $1:season::varchar
    FROM @laliga_statistics.public.stage_historical_classification
  );

COPY INTO scorers
  FROM (
    SELECT $1:player::varchar,
           $1:goals::integer,
           $1:season::varchar,
           $1:real_position::integer
    FROM @laliga_statistics.public.stage_scorers
  );

COPY INTO historical_positions
  FROM (
    SELECT $1:team::varchar,
           $1:fixture::integer,
           $1:position::integer,
           $1:season::varchar
    FROM @laliga_statistics.public.stage_historical_position
  );

COPY INTO results
  FROM (
    SELECT $1:local::varchar,
           $1:away::varchar,
           $1:result::varchar,
           $1:local_goals::integer,
           $1:away_goals::integer,
           $1:season::varchar
    FROM @laliga_statistics.public.stage_results
  );