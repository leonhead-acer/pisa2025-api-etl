
DROP SCHEMA IF EXISTS oat CASCADE;

CREATE SCHEMA oat 

    AUTHORIZATION postgres; 

  
COMMENT ON SCHEMA oat 

    IS 'data from oat'; 

-- Sequence: deliveries
 
CREATE SEQUENCE IF NOT EXISTS public.deliveries_rowId_seq
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

ALTER SEQUENCE public.deliveries_rowId_seq
    OWNER TO postgres;

GRANT ALL ON SEQUENCE public.deliveries_rowId_seq TO postgres;
 

-- Table: oat.deliveries 

DROP TABLE IF EXISTS oat.deliveries; 

CREATE TABLE IF NOT EXISTS oat.deliveries 

( 

  "rowId" bigint NOT NULL DEFAULT nextval('deliveries_rowId_seq'::regclass), 

  "id" character varying(30) COLLATE pg_catalog."default", 

  "testQtiId" character varying(30) COLLATE pg_catalog."default", 

  "testQtiLabel" character varying(50) COLLATE pg_catalog."default", 

  "testQtiTitle" character varying(50) COLLATE pg_catalog."default", 

  "rawData" json, 

  "insertedDate" timestamp with time zone DEFAULT now(),

  CONSTRAINT deliveries_pkey PRIMARY KEY ("rowId") 

)

  

TABLESPACE pg_default; 

  

ALTER TABLE IF EXISTS oat.deliveries 

    OWNER to postgres; 

-- Sequence: deliveryExecutions
 
CREATE SEQUENCE IF NOT EXISTS public.deliveryExecutions_rowId_seq
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

ALTER SEQUENCE public.deliveryExecutions_rowId_seq
    OWNER TO postgres;

GRANT ALL ON SEQUENCE public.deliveryExecutions_rowId_seq TO postgres;
 

-- Table: oat.deliveryExecutions 

DROP TABLE IF EXISTS oat."deliveryExecutions"; 

CREATE TABLE IF NOT EXISTS oat."deliveryExecutions" 

( 

  "rowId" bigint NOT NULL DEFAULT nextval('deliveryExecutions_rowId_seq'::regclass), 

  "deliveryExecutionId" character varying(150) COLLATE pg_catalog."default", 

  "contextId" character varying(30) COLLATE pg_catalog."default", 

  "login" character varying(50) COLLATE pg_catalog."default", 

  "lastUpdateDate" timestamp with time zone, 

  "status" character varying(20) COLLATE pg_catalog."default", 

  "finishTime" time with time zone, 

  "rawData" json,

  CONSTRAINT "deliveryExecutions_pkey" PRIMARY KEY ("rowId") 

) 

  

TABLESPACE pg_default; 

  

ALTER TABLE IF EXISTS oat."deliveryExecutions" 

    OWNER to postgres; 

 
-- Sequence: deliveryExecutions
 
CREATE SEQUENCE IF NOT EXISTS public.deliveryResults_rowId_seq
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

ALTER SEQUENCE public.deliveryResults_rowId_seq
    OWNER TO postgres;

GRANT ALL ON SEQUENCE public.deliveryResults_rowId_seq TO postgres;
 
 

-- Table: oat.deliveryResults 

  

DROP TABLE IF EXISTS oat."deliveryResults"; 

  

CREATE TABLE IF NOT EXISTS oat."deliveryResults" 

( 

    "rowId" bigint NOT NULL DEFAULT nextval('deliveryResults_rowId_seq'::regclass), 

    "deliveryExecutionId" character varying(150) COLLATE pg_catalog."default", 

    "batteryId" character varying(50) COLLATE pg_catalog."default", 

    "deliveryId" character varying(50) COLLATE pg_catalog."default", 

    "login" character varying(50) COLLATE pg_catalog."default", 

    "lastUpdateDate" timestamp with time zone, 

    "isDeleted" bit(1), 

    "outcomes" character varying(200),

    "testQtiId" character varying(30) COLLATE pg_catalog."default", 

    "testQtiLabel" character varying(50) COLLATE pg_catalog."default", 

    "testQtiTitle" character varying(50) COLLATE pg_catalog."default", 

    "insertedDate" timestamp with time zone, 

    "raw_data" character varying (100000),

    CONSTRAINT "deliveryResults_pkey" PRIMARY KEY ("rowId") 

) 

  

TABLESPACE pg_default; 

  

ALTER TABLE IF EXISTS oat."deliveryResults" 

    OWNER to postgres; 

 