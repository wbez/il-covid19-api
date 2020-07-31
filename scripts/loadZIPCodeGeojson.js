const fs = require('fs');
const path = require('path');
const { query } = require('../lib/api');

const directoryPath = path.join('./data/geo');

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

const objects = parsedData.features.map(d => {
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
        variables: { objects }
    })
    .then((response) => console.log(response))
    .catch((error) => console.error(error));    