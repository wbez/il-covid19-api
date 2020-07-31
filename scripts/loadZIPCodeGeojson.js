const dotenv = require('dotenv');
const fs = require('fs');
const path = require('path');
const slugify = require("slugify")
const { query } = require('graphqurl');

dotenv.config();

const directoryPath = path.join('./data/geo');

const APIConfig = {
    endpoint: process.env.HASURA_API_ENDPOINT,
    headers: {
        'x-hasura-access-key': process.env.HASURA_API_SECRET
    }
}

geoQuery = `
    mutation($objects: [census_zipcodes_insert_input!]!) {
        insert_census_zipcodes(
            objects: $objects 
        ) {
            affected_rows
        }
    }
`

const data = fs.readFileSync(path.join(directoryPath, 'il-zcta.json'), 'utf-8')
const parsedData = JSON.parse(data);

// function flattenCensusDemographics(accumulator, [category, demographics]) {
//     if (['Race', 'Population'].includes(category)) {
//         const categorySlug = slugify(category, { replacement: '', strict: true, lower: true });
//         Array.from(Object.entries(demographics)).forEach( ([demographic, values]) => {
//             const demographicSlug = slugify(demographic, { replacement: '', strict: true, lower: true });
//             const slug = `${categorySlug}_${demographicSlug}`;
//             accumulator[`${slug}_estimate`] = values.E;
//             accumulator[`${slug}_estimate_moe`] = values.M;
//             accumulator[`${slug}_pct`] = values.PE;
//             accumulator[`${slug}_pct_moe`] = values.PM;
//         })
//     }
//     return accumulator;
// }

const objects = parsedData.features.map(d => {

    // This is annoying!
    // const demographicsRow = Array.from(Object.entries(d.properties.census_demographics)).reduce(flattenCensusDemographics, {})

    return {
        geom: d.geometry,
        zcta: d.properties.ZCTA5CE10,
        city: d.properties.city,
        county: d.properties.county,
        aland10: d.properties.ALAND10,
        awater10: d.properties.AWATER10,
        race: d.properties.census_demographics.Race,
        population: d.properties.census_demographics.Population,
    }
});

query({
        query: geoQuery,
        variables: { objects },
        ...APIConfig
    })
    .then((response) => console.log(response))
    .catch((error) => console.error(error));    