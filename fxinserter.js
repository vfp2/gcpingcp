/**
 * I am the fxinserter(.js)
 * 
 * I run by:
 * $ npm install
 * $ pip install duka
 * Insert "S1 = 1" to add support for 1 second candlestick timeframes to line 16 @ /System/Volumes/Data/Users/simon/.pyenv/versions/3.8.2/lib/python3.8/site-packages/duka/core/utils.py
 * $ export GOOGLE_APPLICATION_CREDENTIALS=bigquery_service_account.json
 * $ node bqinserter.js <days to add> <start date> <currency pair>
 * 
 * bigquery_service_account.json is gotten by signing up to Google Cloud Platform (GCP), creating
 * a Service Account (under IAM and Admin) with the BigQuery Admin role (ouch, could do with less),
 * adding a JSON key and saving the file in the same folder as I The Inserter.
 * 
 * So what does I The FX Inserter Do?
 * I take forex market data provided by Dukascopy via a tool called duka (https://github.com/giuse88/duka)
 * and insert it into BQ.
 * 
 * by fp2.dev
 */

var moment = require('moment');
const _ = require('lodash');
const csvparser = require('csv-parse');
const fs = require('fs');
const { exec } = require("child_process");

const {BigQuery} = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
const datasetId = 'financial_us';
const tableId = 'fx_market_data_duka';

function execCmd(getDate, currencyPair, i) {
  var cmd = `duka ${currencyPair.toUpperCase()} -c S1 -d ${getDate.format('YYYY-MM-DD')}`;
  console.log(cmd);
  exec(cmd, (error, stdout, stderr) => {
    if (error) {
        console.log(`duka ${currencyPair}/${getDate} error: ${error.message}`);
        return;
    }
    if (stderr) {
      i++;
      console.log(`RERUNNING duka ${currencyPair}/${getDate} coz of stderr: ${i}`);
      execCmd(getDate, currencyPair, i)
    }
    console.log(`duka ${currencyPair}/${getDate} stdout: ${stdout}`);

    fs.readFile(`${currencyPair.toUpperCase()}-${getDate.format('YYYY-MM-DD').replaceAll('-', '_')}-${getDate.format('YYYY-MM-DD').replaceAll('-', '_')}.csv`, 'utf8', function (error, data) {
      if (error) {
        console.log(`read ${currencyPair}/${getDate} error: ${error.message}`);
        return;
      }
      extractAndInsertEntropy(data, currencyPair.toUpperCase());
    });
  });
}

function extractAndInsertEntropy(data, currencyPair) {
  csvparser(
      data,
      async function (csvParseError, records) {
          if (csvParseError) {
              console.error(csvParseError);
              return;
          }

          // iterate all lines
          const rowsForBQ = [];
          records.forEach(async function (record) {
            var rowObj = {};
            var i = 0;
            _.slice(record, 1) // skip time and pair fields
            .forEach(function (samplit) {
              rowObj = {
                time: record[0],
                pair: currencyPair,
                open: record[1],
                close: record[2],
                high: record[3],
                low: record[4],
              };
              i++;
            });

            // console.log(rowObj);
            rowsForBQ.push(rowObj);
          });

          // max 10,000 rows insert/time on BQ
          let index = 0;
          let size = 10000; // maybe: "error": {\n' + "code": 400,\n' +' "message": "Request payload size exceeds the limit: 10485760 bytes.",\n' +
    
          while (index < rowsForBQ.length) {
            let chunk = rowsForBQ.slice(index, size + index);
            await bigquery.dataset(datasetId).table(tableId).insert(chunk);
            //console.log(`Inserted ${chunk.length} rows (max chunker)`);
            index += size;
          }
      });
}

function insert(addDays = 0, startDate = null) {
  async function insertAsync(addDays = 0, startDate = null, currencyPair = 'EURUSD') {
    try {
      await bigquery.createDataset(datasetId);
    } catch {}

    try {
      const schema = [
        { name: 'time', type: 'TIMESTAMP', mode: 'REQUIRED' },
        { name: 'pair', type: 'STRING' },
        { name: 'open', type: 'NUMERIC' },
        { name: 'close', type: 'NUMERIC' },
        { name: 'high', type: 'NUMERIC' },
        { name: 'low', type: 'NUMERIC' },
      ];

      const options = {
        schema: schema,
        location: 'US',
        // timePartitioning: {
        //   type: 'MONTH',
        //   field: 'time',
        // },
        // clustering: {
        //     fields: [
        //       "pair",
        //       "time",
        //     ]
        //   }
      };

      await bigquery.dataset(datasetId).createTable(tableId, options);
    } catch {}

    var getDate = moment(startDate, 'YYYY-MM-DD')
    getDate = getDate.add(addDays, 'd');

    execCmd(getDate, currencyPair, 0);
};

  insertAsync(addDays, startDate);
}

insert(...process.argv.slice(2));