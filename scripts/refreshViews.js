const dotenv = require("dotenv");
const fetch = require("node-fetch");

dotenv.config();

const MATERIALIZED_VIEWS = [
  "public.county_testing_results_change",
  "public.state_testing_results_change",
];

async function refreshMaterializedViews() {
  await fetch(process.env.HASURA_API_ENDPOINT.replace("graphql", "query"), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-hasura-access-key": process.env.HASURA_API_SECRET,
      "x-hasura-role": "admin",
    },
    body: JSON.stringify({
      type: "run_sql",
      args: {
        sql: MATERIALIZED_VIEWS.map(
          (view) => `REFRESH MATERIALIZED VIEW ${view};`
        ).join("\n"),
      },
    }),
  })
    .then((res) => res.json())
    .then((res) => console.log(JSON.stringify(res, null, 2)))
    .catch(console.error);
}

refreshMaterializedViews();
