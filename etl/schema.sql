
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



CREATE TABLE IF NOT EXISTS oat."isot_table" 

( 

    "isoalpha2" character varying(2) COLLATE pg_catalog."default", 

    "isorgncd" character varying(3) COLLATE pg_catalog."default", 

    "isosubrcd" character varying(3) COLLATE pg_catalog."default", 

    "isoind" character varying(1) COLLATE pg_catalog."default", 

    "isoent" character varying(6) COLLATE pg_catalog."default", 

    "isoname" character varying(200),

    "isoalpha3" character varying(3) COLLATE pg_catalog."default", 

    "isoregion" character varying(20) COLLATE pg_catalog."default",

    "isocntcd" character varying(3) COLLATE pg_catalog."default", 

    "isosubrgn" character varying(100) COLLATE pg_catalog."default"

) 

CREATE TABLE IF NOT EXISTS oat."maple_student_post_val" 

(
    "srtrack" character varying(4) COLLATE pg_catalog."default"

    "idinp" character varying(4) COLLATE pg_catalog."default"

    "grade" character varying(2) COLLATE pg_catalog."default"

    "gender" character varying(2) COLLATE pg_catalog."default"

    "dob_mm" character varying(9) COLLATE pg_catalog."default"

    "dob_yy" character varying(4) COLLATE pg_catalog."default"

    "stprog" character varying(2) COLLATE pg_catalog."default"

    "sen" character varying(2) COLLATE pg_catalog."default"

    "isoname" character varying(52) COLLATE pg_catalog."default"

    "isoalpha3" character varying(3) COLLATE pg_catalog."default"

    "isoalpha2" character varying(2) COLLATE pg_catalog."default"

    "mpop1" character varying(1) COLLATE pg_catalog."default"

    "originalRowIndex" character varying(4) COLLATE pg_catalog."default"

    "samplingGroup" character varying(5) COLLATE pg_catalog."default"

    "samplingGroup_decimal" character varying(2) COLLATE pg_catalog."default"

    "sortingColumn" character varying(18) COLLATE pg_catalog."default"

    "sortingSequence" character varying(4) COLLATE pg_catalog."default"

    "output" character varying(7) COLLATE pg_catalog."default"

    "username" character varying(11) COLLATE pg_catalog."default"

    "password" character varying(6) COLLATE pg_catalog."default"

    "instrtp1_id" character varying(3) COLLATE pg_catalog."default"

    "instrtp1_label" character varying(5) COLLATE pg_catalog."default"

    "instrtp2_id" character varying(1) COLLATE pg_catalog."default"

    "instrtp2_label" character varying(4) COLLATE pg_catalog."default"

    "doa_dd" character varying(2) COLLATE pg_catalog."default"

    "doa_mm" character varying(2) COLLATE pg_catalog."default"

    "ppart1" character varying(1) COLLATE pg_catalog."default"

    "testAttendance" character varying(1) COLLATE pg_catalog."default"

    "questionnaireAttendance" character varying(1) COLLATE pg_catalog."default"

    "mpop2" character varying(1) COLLATE pg_catalog."default"

    "uh_extra_time" character varying(1) COLLATE pg_catalog."default"

    "assessmentLang" character varying(1) COLLATE pg_catalog."default"

    "mpop4" character varying(1) COLLATE pg_catalog."default"

    "mpop5" character varying(1) COLLATE pg_catalog."default"
)

