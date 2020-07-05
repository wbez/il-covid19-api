CREATE TABLE public.zipcode_date_counts (
    zipcode text NOT NULL,
    date date NOT NULL,
    confirmed_cases integer NOT NULL,
    total_tested integer NOT NULL,
    id integer NOT NULL
);
CREATE SEQUENCE public.zipcode_date_counts_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.zipcode_date_counts_id_seq OWNED BY public.zipcode_date_counts.id;
CREATE TABLE public.zipcode_date_race_demographics (
    id integer NOT NULL,
    category text NOT NULL,
    count integer,
    total_tested integer,
    zipcode text NOT NULL,
    date date NOT NULL
);
CREATE SEQUENCE public.zipcode_date_race_demographics_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.zipcode_date_race_demographics_id_seq OWNED BY public.zipcode_date_race_demographics.id;
CREATE TABLE public.zipcodes (
    zipcode text NOT NULL
);
CREATE TABLE public.zipcodes_geo (
    id integer NOT NULL,
    zipcode text NOT NULL,
    city text,
    county text,
    aland10 numeric,
    awater10 numeric,
    geom public.geometry NOT NULL
);
CREATE SEQUENCE public.zipcodes_geo_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.zipcodes_geo_id_seq OWNED BY public.zipcodes_geo.id;
ALTER TABLE ONLY public.zipcode_date_counts ALTER COLUMN id SET DEFAULT nextval('public.zipcode_date_counts_id_seq'::regclass);
ALTER TABLE ONLY public.zipcode_date_race_demographics ALTER COLUMN id SET DEFAULT nextval('public.zipcode_date_race_demographics_id_seq'::regclass);
ALTER TABLE ONLY public.zipcodes_geo ALTER COLUMN id SET DEFAULT nextval('public.zipcodes_geo_id_seq'::regclass);
ALTER TABLE ONLY public.zipcode_date_counts
    ADD CONSTRAINT zipcode_date_counts_id_key UNIQUE (id);
ALTER TABLE ONLY public.zipcode_date_counts
    ADD CONSTRAINT zipcode_date_counts_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.zipcode_date_counts
    ADD CONSTRAINT zipcode_date_counts_zipcode_date_key UNIQUE (zipcode, date);
ALTER TABLE ONLY public.zipcode_date_race_demographics
    ADD CONSTRAINT zipcode_date_race_demographics_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.zipcode_date_race_demographics
    ADD CONSTRAINT zipcode_date_race_demographics_zipcode_date_key UNIQUE (zipcode, date);
ALTER TABLE ONLY public.zipcodes_geo
    ADD CONSTRAINT zipcodes_geo_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.zipcodes
    ADD CONSTRAINT zipcodes_pkey PRIMARY KEY (zipcode);
