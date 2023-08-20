/**
 * I am the addEmptyColumn(.js)
 * 
 * I run by:
 * $ npm install
 * $ export GOOGLE_APPLICATION_CREDENTIALS=bigquery_service_account.json
 * $ node addEmptyColumn.js <BigQuery dataset name> <BigQuery table name> <XXXX>
 * 
 * 
 * bigquery_service_account.json is gotten by signing up to Google Cloud Platform (GCP), creating
 * a Service Account (under IAM and Admin) with the BigQuery Admin role (ouch, could do with less),
 * adding a JSON key and saving the file in the same folder as I The Inserter.
 * 
 * So what does I The addEmptyColumn Do?
 * I add a new column to BigQuery for egg_XXXX. XXXX is the name of the new column to add into the table to
 * represent a new EGG (REG) added to GCP network.
 * 
 * by fp2.dev
 */

function main(datasetId = 'eggs', tableId = 'basket_data',  eggNumber = 'egg_') {
  // [START bigquery_add_empty_column]

  // Import the Google Cloud client library and create a client
  const {BigQuery} = require('@google-cloud/bigquery');
  const bigquery = new BigQuery();

  async function addEmptyColumn() {
    // Adds an empty column to the schema.

    const column = {name: `egg_${eggNumber}`, type: 'BYTES' };

    // Retrieve current table metadata
    const table = bigquery.dataset(datasetId).table(tableId);
    const [metadata] = await table.getMetadata();

    // Update table schema
    const schema = metadata.schema;
    const new_schema = schema;
    new_schema.fields.push(column);
    metadata.schema = new_schema;

    const [result] = await table.setMetadata(metadata);
    console.log(result.schema.fields);
    console.log("added: " + JSON.stringify(column));
  }
  // [END bigquery_add_empty_column]
  addEmptyColumn();
}

main(...process.argv.slice(2));
