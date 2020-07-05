const dotenv = require('dotenv');
const fs = require('fs');
const path = require('path');
const { query } = require('graphqurl');

dotenv.config();

const directoryPath = path.join('./data/zipcodes');

const APIConfig = {
    endpoint: process.env.HASURA_API_ENDPOINT,
    headers: {
        'x-hasura-access-key': process.env.HASURA_API_SECRET
    }
}

zipQuery = `
    mutation($objects: [zipcodes_insert_input!]!) {
        insert_zipcodes(
            objects: $objects,  
            on_conflict: {
                constraint: zipcodes_pkey,
                update_columns: [zipcode]
            }
        ) {
            affected_rows
        }
    }
`

async function loadDay(zipData) {
    const updatedDate = `${zipData.LastUpdateDate.year}-${zipData.LastUpdateDate.month}-${zipData.LastUpdateDate.day}`;

    console.log(`loading ${updatedDate}`)

    const objects = zipData.zip_values.map(d => ({
        zipcode: d.zip,
        date_counts: {
            data: [{
                date: updatedDate,
                confirmed_cases: d.confirmed_cases,
                total_tested: d.total_tested,
            }],
            on_conflict: {
                constraint: 'zipcode_date_counts_zipcode_date_key',
                update_columns: ['confirmed_cases', 'total_tested']
            }
        },
        // race_demographics_by_date: {
        //     data: [{
        //         date: updatedDate,
        //         demographics: d.demographics.race
        //     }],
        //     on_conflict: {
        //         constraint: 'zipcode_date_race_demographics_zipcode_date_key',
        //         update_columns: ['demographics']
        //     }
        // },
        // age_demographics_by_date: {
        //     data: [{
        //         date: updatedDate,
        //         demographics: d.demographics.age
        //     }],
        //     on_conflict: {
        //         constraint: 'zipcode_date_age_demographics_zipcode_date_key',
        //         update_columns: ['demographics']
        //     }
        // }
    }))

    return query({
        query: zipQuery,
        variables: { objects },
        ...APIConfig
    })
    .then((response) => console.log(response))
    .catch((error) => console.error(error));    
}

async function loadFile(file) {
    const data = fs.readFileSync(path.join(directoryPath, file), 'utf-8')
    // , (err, data) => {
        // if (err) throw err;
        const parsedData = JSON.parse(data);
        const keys = Array.from(Object.keys(parsedData));
        let zipData;
        if (keys.includes("2020-06-17")) {
            zipData = Array.from(Object.values(parsedData))
        } else {
            zipData = [parsedData]
        }
        // zipData.forEach(loadDay)
        // loadDay(zipData[40]);
        for (const day of zipData) {
            await loadDay(day);
        }
    // })
}

fs.readdir(directoryPath, (err, files) => {
    if (err) throw err;
    files.forEach(loadFile)
})