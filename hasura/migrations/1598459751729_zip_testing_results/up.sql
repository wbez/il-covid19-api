CREATE TABLE public.zipcode_testing_results (
    id serial,
    date date NOT NULL,
    zipcode text NOT NULL,
    confirmed_cases integer,
    total_tested integer,
    census_geography_id integer,
    FOREIGN KEY (census_geography_id) REFERENCES public.census_geographies (id)
);

ALTER TABLE ONLY public.zipcode_testing_results
    ADD CONSTRAINT zipcode_testing_results_pk PRIMARY KEY (id);
ALTER TABLE ONLY public.zipcode_testing_results
    ADD CONSTRAINT zipcode_testing_results_date_zipcode_key UNIQUE (date, zipcode);

INSERT INTO public.zipcode_testing_results(zipcode, date, confirmed_cases, total_tested)
SELECT
    zipcode,
    date,
    confirmed_cases,
    total_tested
FROM public.zipcode_date_total_counts;
