const dotenv = require("dotenv");
const fs = require("fs");
const census = require("citysdk");
const { query } = require("../lib/api");
const { STATE, COUNTY, PLACE, ZCTA } = require("../lib/constants");

dotenv.config();

const CENSUS_RESOLUTION = "500k";
const CENSUS_VINTAGE = 2018;
const CENSUS_SOURCE_PATH = ["acs", "acs5", "profile"];
const CITYSDK_KEY = process.env.CITYSDK_KEY;
const CENSUS_VALUES = [
  "NAME",
  "DP05_0001E",
  "DP05_0001M",
  "DP05_0001PE",
  "DP05_0001PM",
  "DP05_0077E",
  "DP05_0077M",
  "DP05_0077PE",
  "DP05_0077PM",
  "DP05_0078E",
  "DP05_0078M",
  "DP05_0078PE",
  "DP05_0078PM",
  "DP05_0079E",
  "DP05_0079M",
  "DP05_0079PE",
  "DP05_0079PM",
  "DP05_0080E",
  "DP05_0080M",
  "DP05_0080PE",
  "DP05_0080PM",
  "DP05_0081E",
  "DP05_0081M",
  "DP05_0081PE",
  "DP05_0081PM",
  "DP05_0071E",
  "DP05_0071M",
  "DP05_0071PE",
  "DP05_0071PM",
  "DP05_0057E",
  "DP05_0057M",
  "DP05_0057PE",
  "DP05_0057PM",
];

const censusQuery = `
    mutation($objects: [census_geographies_insert_input!]!) {
        insert_census_geographies(
            objects: $objects,
            on_conflict: {
              constraint: census_geographies_geoid_geography_unique,
              update_columns: [name, city, county, zcta, population, age, race, geom]
            }
        ) {
            affected_rows
        }
    }
`;

const censusPromise = (args) =>
  new Promise((resolve, reject) =>
    census(args, (err, json) => (err ? reject(err) : resolve(json)))
  );

const createGeoid = (result) => {
  if ("zip-code-tabulation-area" in result) {
    return result["zip-code-tabulation-area"];
  } else if ("county" in result) {
    return `${result.state}${result.county}`;
  } else if ("place" in result) {
    return `${result.state}${result.place}`;
  } else {
    return result.state;
  }
};

const processGeography = (result) => {
  if ("zip-code-tabulation-area" in result) {
    return ZCTA;
  } else if ("county" in result) {
    return COUNTY;
  } else if ("place" in result) {
    return PLACE;
  } else {
    return STATE;
  }
};

const processName = ({ NAME }) =>
  NAME.replace("ZCTA5 ", "")
    .replace(" County", "")
    .replace(" city", "")
    .replace(", Illinois", "")
    .trim();

const shouldUseGeography = (properties) =>
  (!properties["zip-code-tabulation-area"] && properties["state"] === "17") ||
  ["60", "61", "62"].includes(
    (properties["zip-code-tabulation-area"] || "").slice(0, 2)
  );

const baseArgs = {
  vintage: CENSUS_VINTAGE,
  sourcePath: CENSUS_SOURCE_PATH,
  statsKey: CITYSDK_KEY,
};

const geoHierarchies = [
  { state: "17" },
  { state: "17", county: "*" },
  { state: "17", place: "14000" },
  { "zip-code-tabulation-area": "*" },
];

const createDataMap = () =>
  Promise.all(
    geoHierarchies.map((geoHierarchy) =>
      censusPromise({
        ...baseArgs,
        geoHierarchy,
        values: CENSUS_VALUES,
      })
    )
  )
    .then((results) =>
      results
        .flat()
        .filter(shouldUseGeography)
        .map((result) => ({
          ...result,
          geoid: createGeoid(result),
          name: processName(result),
          geography: processGeography(result),
          zcta:
            "zip-code-tabulation-area" in result
              ? result["zip-code-tabulation-area"]
              : null,
          city: "place" in result ? result.place : null,
        }))
        .reduce(
          (
            acc,
            {
              geoid,
              name,
              geography,
              city,
              county,
              zcta,
              DP05_0001E,
              DP05_0001M,
              DP05_0001PE,
              DP05_0001PM,
              DP05_0077E,
              DP05_0077M,
              DP05_0077PE,
              DP05_0077PM,
              DP05_0078E,
              DP05_0078M,
              DP05_0078PE,
              DP05_0078PM,
              DP05_0079E,
              DP05_0079M,
              DP05_0079PE,
              DP05_0079PM,
              DP05_0080E,
              DP05_0080M,
              DP05_0080PE,
              DP05_0080PM,
              DP05_0081E,
              DP05_0081M,
              DP05_0081PE,
              DP05_0081PM,
              DP05_0071E,
              DP05_0071M,
              DP05_0071PE,
              DP05_0071PM,
              DP05_0057E,
              DP05_0057M,
              DP05_0057PE,
              DP05_0057PM,
            }
          ) => ({
            ...acc,
            [geoid]: {
              geoid,
              name,
              geography,
              city,
              county,
              zcta,
              population: {
                E: DP05_0001E,
                M: DP05_0001M,
                PE: DP05_0001PE,
                PM: DP05_0001PM,
              },
              race: {
                "AI/AN**": {
                  E: DP05_0079E,
                  M: DP05_0079M,
                  PE: DP05_0079PE,
                  PM: DP05_0079PM,
                },
                Asian: {
                  E: DP05_0080E,
                  M: DP05_0080M,
                  PE: DP05_0080PE,
                  PM: DP05_0080PM,
                },
                Black: {
                  E: DP05_0078E,
                  M: DP05_0078M,
                  PE: DP05_0078PE,
                  PM: DP05_0078PM,
                },
                Hispanic: {
                  E: DP05_0071E,
                  M: DP05_0071M,
                  PE: DP05_0071PE,
                  PM: DP05_0071PM,
                },
                "NH/PI*": {
                  E: DP05_0081E,
                  M: DP05_0081M,
                  PE: DP05_0081PE,
                  PM: DP05_0081PM,
                },
                Other: {
                  E: DP05_0057E,
                  M: DP05_0057M,
                  PE: DP05_0057PE,
                  PM: DP05_0057PM,
                },
                White: {
                  E: DP05_0077E,
                  M: DP05_0077M,
                  PE: DP05_0077PE,
                  PM: DP05_0077PM,
                },
              },
            },
          }),
          {}
        )
    )
    .catch(console.error);

const loadCensusGeographies = () =>
  Promise.all(
    geoHierarchies.map((geoHierarchy) =>
      censusPromise({
        ...baseArgs,
        geoHierarchy,
        values: ["NAME"],
        geoResolution: CENSUS_RESOLUTION,
      })
    )
  )
    .then((results) =>
      results
        .flat()
        .map(({ features }) => features)
        .flat()
        .filter(({ properties }) => shouldUseGeography(properties))
    )
    .catch(console.error);

async function loadCensus() {
  const geoidDataMap = await createDataMap();
  const censusGeographies = await loadCensusGeographies();
  const censusDataGeographies = censusGeographies.map(
    ({ properties, geometry }) => ({
      geom: geometry,
      ...(geoidDataMap[createGeoid(properties)] || {}),
    })
  );

  await query({
    query: censusQuery,
    variables: {
      objects: censusDataGeographies,
    },
  })
    .then(console.log)
    .catch(console.error);
}

loadCensus();
