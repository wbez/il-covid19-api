CREATE TABLE public.census_zipcodes (
    id integer NOT NULL,
    zcta text NOT NULL,
    city text,
    county text,
    aland10 numeric,
    awater10 numeric,
    geom public.geometry NOT NULL
);
CREATE TABLE public.zipcode_date_age_counts (
    zipcode text NOT NULL,
    date date NOT NULL,
    confirmed_cases_unknown integer,
    total_tested_unknown integer,
    confirmed_cases_less_than_20 integer,
    total_tested_less_than_20 integer,
    confirmed_cases_20_to_29 integer,
    total_tested_20_to_29 integer,
    confirmed_cases_30_to_39 integer,
    total_tested_30_to_39 integer,
    confirmed_cases_40_to_49 integer,
    total_tested_40_to_49 integer,
    confirmed_cases_50_to_59 integer,
    total_tested_50_to_59 integer,
    confirmed_cases_60_to_69 integer,
    total_tested_60_to_69 integer,
    confirmed_cases_70_to_79 integer,
    total_tested_70_to_79 integer,
    confirmed_cases_80_or_more integer,
    total_tested_80_or_more integer,
    id integer NOT NULL
);
CREATE SEQUENCE public.zipcode_date_age_counts_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.zipcode_date_age_counts_id_seq OWNED BY public.zipcode_date_age_counts.id;
CREATE TABLE public.zipcode_date_total_counts (
    id integer NOT NULL,
    zipcode text NOT NULL,
    date date NOT NULL,
    confirmed_cases integer NOT NULL,
    total_tested integer NOT NULL
);
CREATE SEQUENCE public.zipcode_date_count_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.zipcode_date_count_id_seq OWNED BY public.zipcode_date_total_counts.id;
CREATE TABLE public.zipcode_date_gender_counts (
    id integer NOT NULL,
    zipcode text NOT NULL,
    date date NOT NULL,
    confirmed_cases_male integer,
    total_tested_male integer,
    confirmed_cases_female integer,
    total_tested_female integer,
    confirmed_cases_unknownleftblank integer,
    total_tested_unknownleftblank integer
);
CREATE SEQUENCE public.zipcode_date_gender_demographics_counts_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.zipcode_date_gender_demographics_counts_id_seq OWNED BY public.zipcode_date_gender_counts.id;
CREATE TABLE public.zipcode_date_race_counts (
    id integer NOT NULL,
    zipcode text NOT NULL,
    date date NOT NULL,
    confirmed_cases_black integer,
    total_tested_black integer,
    confirmed_cases_asian integer,
    total_tested_asian integer,
    confirmed_cases_hispanic integer,
    total_tested_hispanic integer,
    confirmed_cases_white integer,
    total_tested_white integer,
    confirmed_cases_leftblank integer,
    total_tested_leftblank integer,
    confirmed_cases_other integer,
    total_tested_other integer,
    confirmed_cases_nhpi integer,
    total_tested_nhpi integer,
    confirmed_cases_aian integer,
    total_tested_aian integer
);
CREATE SEQUENCE public.zipcode_date_race_demographic_counts_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.zipcode_date_race_demographic_counts_id_seq OWNED BY public.zipcode_date_race_counts.id;
CREATE TABLE public.zipcode_dates (
    zipcode text NOT NULL,
    date date NOT NULL
);
CREATE TABLE public.zipcodes (
    zipcode text NOT NULL
);
CREATE SEQUENCE public.zipcodes_geo_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.zipcodes_geo_id_seq OWNED BY public.census_zipcodes.id;
ALTER TABLE ONLY public.census_zipcodes ALTER COLUMN id SET DEFAULT nextval('public.zipcodes_geo_id_seq'::regclass);
ALTER TABLE ONLY public.zipcode_date_age_counts ALTER COLUMN id SET DEFAULT nextval('public.zipcode_date_age_counts_id_seq'::regclass);
ALTER TABLE ONLY public.zipcode_date_gender_counts ALTER COLUMN id SET DEFAULT nextval('public.zipcode_date_gender_demographics_counts_id_seq'::regclass);
ALTER TABLE ONLY public.zipcode_date_race_counts ALTER COLUMN id SET DEFAULT nextval('public.zipcode_date_race_demographic_counts_id_seq'::regclass);
ALTER TABLE ONLY public.zipcode_date_total_counts ALTER COLUMN id SET DEFAULT nextval('public.zipcode_date_count_id_seq'::regclass);
ALTER TABLE ONLY public.zipcode_date_age_counts
    ADD CONSTRAINT zipcode_date_age_counts_id_key UNIQUE (id);
ALTER TABLE ONLY public.zipcode_date_age_counts
    ADD CONSTRAINT zipcode_date_age_counts_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.zipcode_date_age_counts
    ADD CONSTRAINT zipcode_date_age_counts_zipcode_date_key UNIQUE (zipcode, date);
ALTER TABLE ONLY public.zipcode_date_total_counts
    ADD CONSTRAINT zipcode_date_count_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.zipcode_date_gender_counts
    ADD CONSTRAINT zipcode_date_gender_counts_zipcode_date_key UNIQUE (zipcode, date);
ALTER TABLE ONLY public.zipcode_date_gender_counts
    ADD CONSTRAINT zipcode_date_gender_demographics_counts_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.zipcode_dates
    ADD CONSTRAINT zipcode_date_pkey PRIMARY KEY (zipcode, date);
ALTER TABLE ONLY public.zipcode_date_race_counts
    ADD CONSTRAINT zipcode_date_race_counts_zipcode_date_key UNIQUE (zipcode, date);
ALTER TABLE ONLY public.zipcode_date_race_counts
    ADD CONSTRAINT zipcode_date_race_demographic_counts_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.zipcode_date_total_counts
    ADD CONSTRAINT zipcode_date_total_counts_zipcode_date_key UNIQUE (zipcode, date);
ALTER TABLE ONLY public.census_zipcodes
    ADD CONSTRAINT zipcodes_geo_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.zipcodes
    ADD CONSTRAINT zipcodes_pkey PRIMARY KEY (zipcode);
