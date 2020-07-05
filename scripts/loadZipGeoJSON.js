const dotenv = require('dotenv');
const fs = require('fs');
const path = require('path');
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
    mutation($objects: [zipcodes_geo_insert_input!]!) {
        insert_zipcodes_geo(
            objects: $objects 
        ) {
            affected_rows
        }
    }
`

const data = fs.readFileSync(path.join(directoryPath, 'il-zcta.json'), 'utf-8')
const parsedData = JSON.parse(data);

const objects = parsedData.map(d => {
    return {
        geom: d.geometry,
        zipcode: d.properties.ZCTA5CE10,
        city: d.properties.city,
        county: d.properties.county,
        aland10: d.properties.ALAND10,
        awater10: d.properties.AWATER10
    }
});

query({
        query: geoQuery,
        variables: { objects },
        ...APIConfig
    })
    .then((response) => console.log(response))
    .catch((error) => console.error(error));    