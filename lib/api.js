const dotenv = require('dotenv');
const { query } = require('graphqurl');

dotenv.config();

const APIConfig = {
    endpoint: process.env.HASURA_API_ENDPOINT,
    headers: {
        'x-hasura-access-key': process.env.HASURA_API_SECRET
    }
}

exports.query = (opts) => {
  return query({
    ...opts,
    ...APIConfig
  });
}