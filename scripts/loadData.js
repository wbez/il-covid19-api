const dotenv = require('dotenv');
const fs = require('fs');
const path = require('path');
const { query } = require('graphqurl');

dotenv.config();

const directoryPath = path.join('./data');

const APIConfig = {
    endpoint: process.env.HASURA_API_ENDPOINT,
    headers: {
        'x-hasura-access-key': process.env.HASURA_API_SECRET
    }
}

zipQuery = `
mutation($objects: [zipcode_date_counts_insert_input!]!) {
    insert_zipcode_date_counts(
        objects: $objects,  
        on_conflict: {
            constraint: zipcode_date_counts_zipcode_date_key,
            update_columns: [date, confirmed_cases, total_tested]
        }
    ) {
        returning {
            zipcode
            confirmed_cases
        }
    }
}
`

async function loadDay(zipData) {
    const updatedDate = `${zipData.LastUpdateDate.year}-${zipData.LastUpdateDate.month}-${zipData.LastUpdateDate.day}`;

    const objects = zipData.zip_values.map(d => ({
        date: updatedDate,
        zipcode: d.zip,
        confirmed_cases: d.confirmed_cases,
        total_tested: d.total_tested,
    }))

    query({
        query: zipQuery,
        variables: { objects },
        ...APIConfig
    })
    .then((response) => console.log(response))
    .catch((error) => console.error(error));    
}

async function loadFile(file) {
    fs.readFile(path.join(directoryPath, file), 'utf-8', (err, data) => {
        if (err) throw err;
        let zipData = JSON.parse(data);
        if (!Array.isArray(data)) {
            zipData = [zipData]
        }
        zipData.forEach(loadDay)
    })
}

fs.readdir(directoryPath, (err, files) => {
    if (err) throw err;
    files.forEach(loadFile)
})