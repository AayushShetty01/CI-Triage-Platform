DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'parser') THEN
    CREATE ROLE parser LOGIN PASSWORD 'parser_dev';
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'classifier') THEN
    CREATE ROLE classifier LOGIN PASSWORD 'classifier_dev';
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ingestion') THEN
    CREATE ROLE ingestion LOGIN PASSWORD 'ingestion_dev';
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'query') THEN
    CREATE ROLE query LOGIN PASSWORD 'query_dev';
  END IF;
END $$;

CREATE SCHEMA IF NOT EXISTS parser AUTHORIZATION parser;
CREATE SCHEMA IF NOT EXISTS classifier AUTHORIZATION classifier;
CREATE SCHEMA IF NOT EXISTS ingestion AUTHORIZATION ingestion;
CREATE SCHEMA IF NOT EXISTS query AUTHORIZATION query;

DO $$
DECLARE
  s text;
  r record;
BEGIN
  FOR s IN VALUES ('parser'), ('classifier'), ('ingestion'), ('query') LOOP
    EXECUTE format('ALTER SCHEMA %I OWNER TO %I', s, s);
    EXECUTE format('GRANT ALL ON SCHEMA %I TO %I', s, s);
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = s) LOOP
      EXECUTE format('ALTER TABLE %I.%I OWNER TO %I', s, r.tablename, s);
      EXECUTE format('GRANT ALL ON TABLE %I.%I TO %I', s, r.tablename, s);
    END LOOP;
    FOR r IN (SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = s) LOOP
      EXECUTE format('ALTER SEQUENCE %I.%I OWNER TO %I', s, r.sequence_name, s);
      EXECUTE format('GRANT ALL ON SEQUENCE %I.%I TO %I', s, r.sequence_name, s);
    END LOOP;
  END LOOP;
END $$;
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'parser') THEN
    CREATE ROLE parser LOGIN PASSWORD 'parser_dev';
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'classifier') THEN
    CREATE ROLE classifier LOGIN PASSWORD 'classifier_dev';
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ingestion') THEN
    CREATE ROLE ingestion LOGIN PASSWORD 'ingestion_dev';
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'query') THEN
    CREATE ROLE query LOGIN PASSWORD 'query_dev';
  END IF;
END $$;

CREATE SCHEMA IF NOT EXISTS parser AUTHORIZATION parser;
CREATE SCHEMA IF NOT EXISTS classifier AUTHORIZATION classifier;
CREATE SCHEMA IF NOT EXISTS ingestion AUTHORIZATION ingestion;
CREATE SCHEMA IF NOT EXISTS query AUTHORIZATION query;

DO $$
DECLARE
  s text;
  r record;
BEGIN
  FOR s IN VALUES ('parser'), ('classifier'), ('ingestion'), ('query') LOOP
    EXECUTE format('ALTER SCHEMA %I OWNER TO %I', s, s);
    EXECUTE format('GRANT ALL ON SCHEMA %I TO %I', s, s);
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = s) LOOP
      EXECUTE format('ALTER TABLE %I.%I OWNER TO %I', s, r.tablename, s);
      EXECUTE format('GRANT ALL ON TABLE %I.%I TO %I', s, r.tablename, s);
    END LOOP;
    FOR r IN (SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = s) LOOP
      EXECUTE format('ALTER SEQUENCE %I.%I OWNER TO %I', s, r.sequence_name, s);
      EXECUTE format('GRANT ALL ON SEQUENCE %I.%I TO %I', s, r.sequence_name, s);
    END LOOP;
  END LOOP;
END $$;

