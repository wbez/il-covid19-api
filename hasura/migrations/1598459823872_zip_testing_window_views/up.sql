CREATE MATERIALIZED VIEW public.zipcode_testing_results_change
AS
    SELECT
    date,
    zipcode,
    census_geography_id,
    total_tested,
    total_tested - lag(total_tested) OVER (PARTITION BY zipcode ORDER BY date) AS total_tested_change,
    round(
        (
            (total_tested - lag(total_tested) OVER (PARTITION BY zipcode ORDER BY date))::numeric / lag(total_tested) OVER (PARTITION BY zipcode ORDER BY date)
        ) * 100,
        2
    ) AS total_tested_change_pct,
    confirmed_cases,
    confirmed_cases - lag(confirmed_cases) OVER (PARTITION BY zipcode ORDER BY date) AS confirmed_cases_change,
    round(
        (
            (confirmed_cases - lag(confirmed_cases) OVER (PARTITION BY zipcode ORDER BY date))::numeric / lag(confirmed_cases) OVER (PARTITION BY zipcode ORDER BY date)
        ) * 100,
        2
    ) AS confirmed_cases_change_pct,
    CASE
        WHEN public.census_geographies.id IS NULL
        THEN NULL
        ELSE round(
            (public.census_geographies.population->>'M')::numeric /
            (public.census_geographies.population->>'E')::numeric,
            2
        )
        END
    AS pct_population_moe,
    CASE
        WHEN public.census_geographies.id IS NULL
        THEN TRUE
        ELSE (
            (public.census_geographies.population->>'M')::numeric /
            (public.census_geographies.population->>'E')::numeric
        ) < 0.1
        END
    AS below_moe_threshold,
    CASE
        WHEN public.census_geographies.id IS NULL THEN NULL
        WHEN (
            (public.census_geographies.population->>'M')::numeric /
            (public.census_geographies.population->>'E')::numeric
        ) < 0.1 THEN round(
                confirmed_cases / (public.census_geographies.population->>'E')::numeric,
                2
            )
        ELSE NULL
        END
    AS cases_per_capita,
    CASE
        WHEN public.census_geographies.id IS NULL THEN NULL
        WHEN (
            (public.census_geographies.population->>'M')::numeric /
            (public.census_geographies.population->>'E')::numeric
        ) < 0.1 THEN round(
            (confirmed_cases / (public.census_geographies.population->>'E')::numeric) * 1000,
            2
        )
        ELSE NULL
        END
    AS cases_per_1000
    FROM public.zipcode_testing_results
        LEFT JOIN public.census_geographies ON public.zipcode_testing_results.census_geography_id = public.census_geographies.id
    GROUP BY date, zipcode, census_geography_id, total_tested, confirmed_cases, public.census_geographies.id
    ORDER BY date DESC;
