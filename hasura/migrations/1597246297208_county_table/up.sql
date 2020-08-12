CREATE TABLE public.county_testing_results (
    id serial,
    date date NOT NULL,
    county text NOT NULL,
    confirmed_cases integer,
    deaths integer,
    total_tested integer,
    census_geography_id integer,
    FOREIGN KEY (census_geography_id) REFERENCES public.census_geographies (id)
);

ALTER TABLE ONLY public.county_testing_results
    ADD CONSTRAINT county_testing_results_pk PRIMARY KEY (id);
ALTER TABLE ONLY public.county_testing_results
    ADD CONSTRAINT county_testing_results_date_county_key UNIQUE (date, county);
