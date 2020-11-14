/**
 * I am the inserter(.js)
 * 
 * I run by:
 * $ npm install
 * $ export GOOGLE_APPLICATION_CREDENTIALS=bigquery_service_account.json
 * $ node inserter.js
 * 
 * bigquery_service_account.json is gotten by signing up to Google Cloud Platform (GCP), creating
 * a Service Account (under IAM and Admin) with the BigQuery Admin role (ouch, could do with less),
 * adding a JSON key and saving the file in the same folder as I The Inserter.
 * 
 * So what does I The Inserter Do?
 * I take all the two decades+ worth of random data from The Globlal Consciousness Project and insert
 * them into in Google's BigQuery.
 * 
 * by fp2.dev
 */

var moment = require('moment');
const _ = require('lodash');
const request = require('request');
const csvparser = require('csv-parse');

const {BigQuery} = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
const datasetId = 'eggs';
const tableId = 'basket_data';

// eg. http://global-mind.org/cgi-bin/eggdatareq.pl?z=1&year=2020&month=1&day=10&stime=00%3A00%3A00&etime=23%3A59%3A59
const API_HOST = "http://global-mind.org";

// http://noosphere.princeton.edu/basket_CSV_v2.html
// Protocol Description: Type 10 Records
// Field 1: Type   Field 2: Item Field 3: Value     Field 4: Comment     Variable
// ...
// 10             4             Trials per sample "Trial size"         $trialsz
// "Trial size" seems fixed at 200 bits/sec so we'll const'ify it
// const TRIAL_SIZE = 200;

function makeRequestUrl(year, month, day, startTime, endTime) {
  var url = API_HOST +
      "/cgi-bin/eggdatareq.pl" +
      "?z=1" + // not sure what this does
      "&year=" + year +
      "&month=" + month +
      "&day=" + day +
      "&stime=" + startTime +
      "&etime=" + endTime;
  console.log("request url:" + url);
  return url;
}

function handleErrors(gcpRequestError, gcpResponse) {
  if (gcpRequestError) {
      console.error(gcpRequestError);
      return true;
  }

  if (gcpResponse.statusCode != 200) {
      console.error("gcpResponse.statusCode != 200: " + gcpResponse.statusCode);
      return true;
  }

  return false;
}

function extractAndInsertEntropy(body) {
  console.log("GCP response size: " + body.length);
  // parse the csv output
  // ref: http://noosphere.princeton.edu/basket_CSV_v2.html
  csvparser(
      body,
      { relax_column_count: true },
      async function (csvParseError, records) {
          if (csvParseError) {
              console.error(csvParseError);
              return;
          }

          // iterate all lines, process only sample egg ID and value rows
          var eggIDs = [];
          var rowsForBQ = [];
          records.forEach(async function (record) {
            var newRow = [];
            if (record[0] == 12) { // field type 12: IDs of EGGs in today's sample set
              _.slice(record, 3) // skip field type, "gmtime" and empty
              .forEach(function(eggID) {
                eggIDs.push("egg_" + eggID);
              });
            }

            if (record[0] == 13) { // field type 13: actual sample data
              var rowObj = { recorded_at: moment(record[1]*1000).utc().format('YYYY-MM-DD HH:mm:ss') };
              var i = 0;
              _.slice(record, 3) // skip field type, unix timestamp, user friendly timestamp columns
              .forEach(function (samplit) {
                var uint8 = new Buffer(1);
                if (samplit) {
                    uint8[0] = samplit;
                    rowObj[eggIDs[i]] = uint8
                }
                i++;
              });

              // console.log(rowObj);
              rowsForBQ.push(rowObj);

              // TODO: temp
              if (rowsForBQ.length == 9999) {
                await bigquery.dataset(datasetId).table(tableId).insert(rowsForBQ);
                console.log(`Inserted ${rowsForBQ.length} rows`);
                return;
              }
            }
          });

          // await bigquery.dataset(datasetId).table(tableId).insert(rowsForBQ);
          // console.log(`Inserted ${rowsForBQ.length} rows`);
      });
}

function insert() {
  async function insertAsync() {

    try {
      await bigquery.createDataset(datasetId);
    } catch {}

    try {
      const schema = [
        { name: 'recorded_at', type: 'DATETIME', mode: 'REQUIRED' },
        { name: 'egg_1', type: 'BYTES' },
        { name: 'egg_28', type: 'BYTES' },
        { name: 'egg_33', type: 'BYTES' },
        { name: 'egg_34', type: 'BYTES' },
        { name: 'egg_37', type: 'BYTES' },
        { name: 'egg_100', type: 'BYTES' },
        { name: 'egg_101', type: 'BYTES' },
        { name: 'egg_102', type: 'BYTES' },
        { name: 'egg_103', type: 'BYTES' },
        { name: 'egg_105', type: 'BYTES' },
        { name: 'egg_106', type: 'BYTES' },
        { name: 'egg_107', type: 'BYTES' },
        { name: 'egg_108', type: 'BYTES' },
        { name: 'egg_109', type: 'BYTES' },
        { name: 'egg_110', type: 'BYTES' },
        { name: 'egg_111', type: 'BYTES' },
        { name: 'egg_112', type: 'BYTES' },
        { name: 'egg_114', type: 'BYTES' },
        { name: 'egg_115', type: 'BYTES' },
        { name: 'egg_116', type: 'BYTES' },
        { name: 'egg_118', type: 'BYTES' },
        { name: 'egg_119', type: 'BYTES' },
        { name: 'egg_134', type: 'BYTES' },
        { name: 'egg_142', type: 'BYTES' },
        { name: 'egg_161', type: 'BYTES' },
        { name: 'egg_224', type: 'BYTES' },
        { name: 'egg_226', type: 'BYTES' },
        { name: 'egg_227', type: 'BYTES' },
        { name: 'egg_228', type: 'BYTES' },
        { name: 'egg_230', type: 'BYTES' },
        { name: 'egg_231', type: 'BYTES' },
        { name: 'egg_233', type: 'BYTES' },
        { name: 'egg_237', type: 'BYTES' },
        { name: 'egg_1000', type: 'BYTES' },
        { name: 'egg_1004', type: 'BYTES' },
        { name: 'egg_1005', type: 'BYTES' },
        { name: 'egg_1013', type: 'BYTES' },
        { name: 'egg_1021', type: 'BYTES' },
        { name: 'egg_1022', type: 'BYTES' },
        { name: 'egg_1023', type: 'BYTES' },
        { name: 'egg_1024', type: 'BYTES' },
        { name: 'egg_1025', type: 'BYTES' },
        { name: 'egg_1026', type: 'BYTES' },
        { name: 'egg_1027', type: 'BYTES' },
        { name: 'egg_1029', type: 'BYTES' },
        { name: 'egg_1051', type: 'BYTES' },
        { name: 'egg_1063', type: 'BYTES' },
        { name: 'egg_1066', type: 'BYTES' },
        { name: 'egg_1070', type: 'BYTES' },
        { name: 'egg_1082', type: 'BYTES' },
        { name: 'egg_1092', type: 'BYTES' },
        { name: 'egg_1095', type: 'BYTES' },
        { name: 'egg_1096', type: 'BYTES' },
        { name: 'egg_1101', type: 'BYTES' },
        { name: 'egg_1113', type: 'BYTES' },
        { name: 'egg_1223', type: 'BYTES' },
        { name: 'egg_1237', type: 'BYTES' },
        { name: 'egg_1245', type: 'BYTES' },
        { name: 'egg_1251', type: 'BYTES' },
        { name: 'egg_1295', type: 'BYTES' },
        { name: 'egg_2000', type: 'BYTES' },
        { name: 'egg_2001', type: 'BYTES' },
        { name: 'egg_2002', type: 'BYTES' },
        { name: 'egg_2006', type: 'BYTES' },
        { name: 'egg_2007', type: 'BYTES' },
        { name: 'egg_2008', type: 'BYTES' },
        { name: 'egg_2009', type: 'BYTES' },
        { name: 'egg_2013', type: 'BYTES' },
        { name: 'egg_2022', type: 'BYTES' },
        { name: 'egg_2023', type: 'BYTES' },
        { name: 'egg_2024', type: 'BYTES' },
        { name: 'egg_2026', type: 'BYTES' },
        { name: 'egg_2027', type: 'BYTES' },
        { name: 'egg_2028', type: 'BYTES' },
        { name: 'egg_2040', type: 'BYTES' },
        { name: 'egg_2041', type: 'BYTES' },
        { name: 'egg_2042', type: 'BYTES' },
        { name: 'egg_2043', type: 'BYTES' },
        { name: 'egg_2044', type: 'BYTES' },
        { name: 'egg_2045', type: 'BYTES' },
        { name: 'egg_2046', type: 'BYTES' },
        { name: 'egg_2047', type: 'BYTES' },
        { name: 'egg_2048', type: 'BYTES' },
        { name: 'egg_2049', type: 'BYTES' },
        { name: 'egg_2052', type: 'BYTES' },
        { name: 'egg_2060', type: 'BYTES' },
        { name: 'egg_2061', type: 'BYTES' },
        { name: 'egg_2062', type: 'BYTES' },
        { name: 'egg_2064', type: 'BYTES' },
        { name: 'egg_2069', type: 'BYTES' },
        { name: 'egg_2070', type: 'BYTES' },
        { name: 'egg_2073', type: 'BYTES' },
        { name: 'egg_2080', type: 'BYTES' },
        { name: 'egg_2083', type: 'BYTES' },
        { name: 'egg_2084', type: 'BYTES' },
        { name: 'egg_2088', type: 'BYTES' },
        { name: 'egg_2091', type: 'BYTES' },
        { name: 'egg_2093', type: 'BYTES' },
        { name: 'egg_2094', type: 'BYTES' },
        { name: 'egg_2097', type: 'BYTES' },
        { name: 'egg_2120', type: 'BYTES' },
        { name: 'egg_2165', type: 'BYTES' },
        { name: 'egg_2173', type: 'BYTES' },
        { name: 'egg_2178', type: 'BYTES' },
        { name: 'egg_2201', type: 'BYTES' },
        { name: 'egg_2202', type: 'BYTES' },
        { name: 'egg_2220', type: 'BYTES' },
        { name: 'egg_2221', type: 'BYTES' },
        { name: 'egg_2222', type: 'BYTES' },
        { name: 'egg_2225', type: 'BYTES' },
        { name: 'egg_2230', type: 'BYTES' },
        { name: 'egg_2231', type: 'BYTES' },
        { name: 'egg_2232', type: 'BYTES' },
        { name: 'egg_2234', type: 'BYTES' },
        { name: 'egg_2235', type: 'BYTES' },
        { name: 'egg_2236', type: 'BYTES' },
        { name: 'egg_2239', type: 'BYTES' },
        { name: 'egg_2240', type: 'BYTES' },
        { name: 'egg_2241', type: 'BYTES' },
        { name: 'egg_2242', type: 'BYTES' },
        { name: 'egg_2243', type: 'BYTES' },
        { name: 'egg_2247', type: 'BYTES' },
        { name: 'egg_2248', type: 'BYTES' },
        { name: 'egg_2249', type: 'BYTES' },
        { name: 'egg_2250', type: 'BYTES' },
        { name: 'egg_3005', type: 'BYTES' },
        { name: 'egg_3023', type: 'BYTES' },
        { name: 'egg_3043', type: 'BYTES' },
        { name: 'egg_3045', type: 'BYTES' },
        { name: 'egg_3066', type: 'BYTES' },
        { name: 'egg_3101', type: 'BYTES' },
        { name: 'egg_3103', type: 'BYTES' },
        { name: 'egg_3104', type: 'BYTES' },
        { name: 'egg_3106', type: 'BYTES' },
        { name: 'egg_3107', type: 'BYTES' },
        { name: 'egg_3108', type: 'BYTES' },
        { name: 'egg_3115', type: 'BYTES' },
        { name: 'egg_3142', type: 'BYTES' },
        { name: 'egg_3240', type: 'BYTES' },
        { name: 'egg_3247', type: 'BYTES' },
        { name: 'egg_4002', type: 'BYTES' },
        { name: 'egg_4101', type: 'BYTES' },
        { name: 'egg_4234', type: 'BYTES' },
        { name: 'egg_4251', type: 'BYTES' },
      ];

      const options = {
        schema: schema,
        location: 'SG'
        location: 'SG',
      };

      await bigquery.dataset(datasetId).createTable(tableId, options);
    } catch {}

    var gcpStartDate = moment('1998-08-05', 'YYYY-MM-DD')
    // var gcpStartDate = moment('2001-09-11', 'YYYY-MM-DD')
    var yearFmt = gcpStartDate.format('YYYY');
    var monthFmt = gcpStartDate.format('MM');
    var dayFmt = gcpStartDate.format('DD');
    var gcpStartReqUrl = makeRequestUrl(yearFmt, monthFmt, dayFmt, '00:00:00', '23:59:59');

    request(gcpStartReqUrl,
      function (gcpRequestError, gcpResponse, todayBody) {
        if (handleErrors(gcpRequestError, gcpResponse)) {
            return;
        }

        extractAndInsertEntropy(todayBody);
      });
  }

  insertAsync();
}

insert();