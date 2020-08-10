const fetch = require("node-fetch");
const { query } = require("../lib/api");

const ENDPOINT = "https://www.dph.illinois.gov/sitefiles/COVIDTestResults.json";

const stateDemographicsQuery = `
  mutation(
    $raceCounts: state_race_counts_insert_input!,
    $ageCounts: state_age_counts_insert_input!,
    $ageRaceCounts: state_age_race_counts_insert_input!,
    $genderCounts: state_gender_counts_insert_input!
  ) {
    insert_state_race_counts(
      objects: [$raceCounts],
      on_conflict: {
        constraint: state_race_counts_date_key,
        update_columns: []
      }
    ) {
      affected_rows
    }
    insert_state_age_counts(
      objects: [$ageCounts],
      on_conflict: {
        constraint: state_age_counts_date_key,
        update_columns: []
      }
    ) {
      affected_rows
    }
    insert_state_age_race_counts(
      objects: [$ageRaceCounts],
      on_conflict: {
        constraint: state_age_race_counts_date_key,
        update_columns: []
      }
    ) {
      affected_rows
    }
    insert_state_gender_counts(
      objects: [$genderCounts],
      on_conflict: {
        constraint: state_gender_counts_date_key,
        update_columns: []
      }
    ) {
      affected_rows
    }
  }
`;

// TODO: Ignoring for now, pending updated Census loading
const countyCountsQuery = `
  mutation($values: [county_testing_results_insert_input!]!) {
    insert_county_testing_results(
      objects: $values,
      on_conflict: {
        constraint: county_testing_results_county_date_key,
        update_columns: [confirmed_cases, deaths, total_tested]
      }
    ) {
      affected_rows
    }
  }
`;

const stateResultQuery = `
  mutation($values: [state_testing_results_insert_input!]!) {
    insert_state_testing_results(
      objects: $values,
      on_conflict: {
        constraint: state_testing_results_date_key,
        update_columns: [total_tested, confirmed_cases, deaths]
      }
    ) {
      affected_rows
    }
  }
`;

const stateRecoveryQuery = `
  mutation($values: [state_recovery_data_insert_input!]!) {
    insert_state_recovery_data(
      objects: $values,
      on_conflict: {
        constraint: state_recovery_data_report_date_key,
        update_columns: [sample_surveyed, recovered_cases, recovered_and_deceased_cases, recovery_rate]
      }
    ) {
      affected_rows
    }
  }
`;

const probableCaseQuery = `
  mutation($values: state_probable_case_counts_insert_input!) {
    insert_state_probable_case_counts(
      objects: [$values],
      on_conflict: {
        constraint: state_probable_case_counts_date_key,
        update_columns: [probable_cases, probable_deaths]
      }
    ) {
      affected_rows
    }
  }
`;

const loadData = (endpoint) =>
  fetch(endpoint)
    .then((res) => res.json())
    .then(
      ({
        LastUpdateDate: { year, month, day },
        characteristics_by_county: { values: countyValues },
        state_testing_results: { values: stateResultValues },
        demographics,
        state_recovery_data: { values: stateRecoveryValues },
        probable_case_counts: probableCaseValues,
      }) => ({
        date: `${year}-${month}-${day}`,
        countyValues,
        stateResultValues,
        demographics,
        stateRecoveryValues,
        probableCaseValues,
      })
    );

// Converts date strings in m/d/yyyy format to yyyy-mm-dd
const transformDate = (dateStr) => {
  const [month, day, year] = dateStr.split("/");
  return [year, month, day].join("-");
};

const demographicsRowSlug = ({ age_group, race, gender }) =>
  [
    age_group
      ? age_group
          .replace("<", "less_than_")
          .replace("+", "_or_more")
          .replace("-", "_to_")
      : null,
    race,
    gender,
  ]
    .filter((s) => !!s)
    .map((s) => s.toLowerCase().replace(/[^a-z0-9\_]/g, ``))
    .join("_");

const flattenDemographics = (
  acc,
  { age_group, race, gender, count, tested, deaths }
) => {
  const slug = demographicsRowSlug({ age_group, race, gender });
  return {
    ...acc,
    [`count_${slug}`]: count,
    [`tested_${slug}`]: tested,
    [`deaths_${slug}`]: deaths,
  };
};

const transformDemographics = ({ age, race, gender }) => [
  ...age.map(({ age_group, count, tested, deaths }) => ({
    age_group,
    race: null,
    gender: null,
    count,
    tested,
    deaths,
  })),
  ...age
    .map(({ age_group, demographics: { race: ageRaceDemographics } }) =>
      ageRaceDemographics.map(({ description, count, tested, deaths }) => ({
        age_group,
        race: description,
        gender: null,
        count,
        tested,
        deaths,
      }))
    )
    .flat(),
  ...race.map(({ description, count, tested, deaths }) => ({
    age_group: null,
    race: description,
    gender: null,
    count,
    tested,
    deaths,
  })),
  ...gender.map(({ description, count, tested, deaths }) => ({
    age_group: null,
    race: null,
    gender: description,
    count,
    tested,
    deaths,
  })),
];

// TODO: Add date into this, how is GEOID pulled?
// TODO: How are we using GEOID for Cook County without Chicago?
const transformCounty = ({
  County: county,
  confirmed_cases,
  deaths,
  total_tested,
}) => ({ county, confirmed_cases, deaths, total_tested });

const transformStateResults = ({
  testDate,
  total_tested,
  confirmed_cases,
  deaths,
}) => ({
  date: transformDate(testDate),
  total_tested,
  confirmed_cases,
  deaths,
});

const transformStateRecovery = ({
  report_date,
  sample_surveyed,
  recovered_cases,
  recovered_and_deceased_cases,
  recovery_rate,
}) => ({
  report_date: transformDate(report_date),
  sample_surveyed,
  recovered_cases,
  recovered_and_deceased_cases,
  recovery_rate,
});

const transformProbableCaseCounts = ({
  LastUpdatedDate: date,
  probable_cases,
  probable_deaths,
}) => ({ date: transformDate(date), probable_cases, probable_deaths });

const runQuery = async ({ queryStr, variables }) =>
  query({ query: queryStr, variables }).then(console.log).catch(console.err);

const updateData = async (queries) =>
  await Promise.all(
    queries.map(([queryStr, variables]) => runQuery({ queryStr, variables }))
  );

loadData(ENDPOINT).then(
  ({
    date,
    demographics: { age, race, gender },
    countyValues,
    stateResultValues,
    stateRecoveryValues,
    probableCaseValues,
  }) => {
    const demographicCounts = transformDemographics({ age, race, gender });
    const raceCounts = demographicCounts
      .filter(({ age_group, race, gender }) => !!race && !(age_group || gender))
      .reduce(flattenDemographics, { date });
    const ageCounts = demographicCounts
      .filter(({ age_group, race, gender }) => !!age_group && !(race || gender))
      .reduce(flattenDemographics, { date });
    const ageRaceCounts = demographicCounts
      .filter(({ age_group, race, gender }) => age_group && race && !gender)
      .reduce(flattenDemographics, { date });
    const genderCounts = demographicCounts
      .filter(({ age_group, race, gender }) => gender && !(age_group || race))
      .reduce(flattenDemographics, { date });

    const stateResults = stateResultValues.map(transformStateResults);
    const stateRecovery = stateRecoveryValues.map(transformStateRecovery);
    const probableCaseCounts = transformProbableCaseCounts(probableCaseValues);

    updateData([
      [
        stateDemographicsQuery,
        { raceCounts, ageCounts, ageRaceCounts, genderCounts },
      ],
      [stateResultQuery, { values: stateResults }],
      [stateRecoveryQuery, { values: stateRecovery }],
      [probableCaseQuery, { values: probableCaseCounts }],
    ]);
  }
);
