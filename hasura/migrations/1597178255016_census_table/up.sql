CREATE TABLE public.census_geographies (
    id serial,
    geoid text NOT NULL,
    geography text NOT NULL,
    name text NOT NULL,
    city text,
    county text,
    zcta text,
    population jsonb NOT NULL default '{}'::jsonb,
    age jsonb NOT NULL default '{}'::jsonb,
    race jsonb NOT NULL default '{}'::jsonb,
    geom public.geometry NOT NULL
);

ALTER TABLE ONLY public.census_geographies
    ADD CONSTRAINT census_geographies_pk PRIMARY KEY (id);
ALTER TABLE ONLY public.census_geographies
    ADD CONSTRAINT census_geographies_geoid_geography_unique UNIQUE (geoid, geography);
