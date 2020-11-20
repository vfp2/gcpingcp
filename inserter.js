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
const fs = require('fs');

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

function makeRequestUrl(date) {
  var year = date.format('YYYY');
  var month = date.format('MM');
  var day = date.format('DD');
  var startTime = '00:00:00';
  var endTime = '23:59:59';


  var url = API_HOST +
      "/cgi-bin/eggdatareq.pl" +
      "?z=1" + // not sure what this does
      "&year=" + year +
      "&month=" + month +
      "&day=" + day +
      "&stime=" + startTime +
      "&etime=" + endTime;
  console.log(url);
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

function extractAndInsertEntropy(body, writeToFile = false) {
  //console.log("GCP response size: " + body.length);
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
          const rowsForBQ = [];
          var yyymmdd;
          records.forEach(async function (record) {
            if (record[0] == 12) { // field type 12: IDs of EGGs in today's sample set
              _.slice(record, 3) // skip field type, "gmtime" and empty
              .forEach(function(eggID) {
                eggIDs.push("egg_" + eggID);
              });
            }

            if (record[0] == 13) { // field type 13: actual sample data
              if (!yyymmdd) {
                yyyymmdd = moment(record[1]*1000).utc().format('YYYY-MM-DD');
              }

              var rowObj = { recorded_at: moment(record[1]*1000).utc().format('YYYY-MM-DD HH:mm:ss') };
              if (writeToFile) {
                rowObj = {
                  ...rowObj,
                  'egg_1': null,
                  'egg_28': null,
                  'egg_33': null,
                  'egg_34': null,
                  'egg_37': null,
                  'egg_100': null,
                  'egg_101': null,
                  'egg_102': null,
                  'egg_103': null,
                  'egg_104': null,
                  'egg_105': null,
                  'egg_106': null,
                  'egg_107': null,
                  'egg_108': null,
                  'egg_109': null,
                  'egg_110': null,
                  'egg_111': null,
                  'egg_112': null,
                  'egg_114': null,
                  'egg_115': null,
                  'egg_116': null,
                  'egg_117': null,
                  'egg_118': null,
                  'egg_119': null,
                  'egg_134': null,
                  'egg_142': null,
                  'egg_161': null,
                  'egg_223': null,
                  'egg_224': null,
                  'egg_226': null,
                  'egg_227': null,
                  'egg_228': null,
                  'egg_230': null,
                  'egg_231': null,
                  'egg_233': null,
                  'egg_237': null,
                  'egg_1000': null,
                  'egg_1003': null,
                  'egg_1004': null,
                  'egg_1005': null,
                  'egg_1013': null,
                  'egg_1021': null,
                  'egg_1022': null,
                  'egg_1023': null,
                  'egg_1024': null,
                  'egg_1025': null,
                  'egg_1026': null,
                  'egg_1027': null,
                  'egg_1029': null,
                  'egg_1051': null,
                  'egg_1063': null,
                  'egg_1066': null,
                  'egg_1070': null,
                  'egg_1082': null,
                  'egg_1092': null,
                  'egg_1095': null,
                  'egg_1096': null,
                  'egg_1101': null,
                  'egg_1113': null,
                  'egg_1223': null,
                  'egg_1237': null,
                  'egg_1245': null,
                  'egg_1251': null,
                  'egg_1295': null,
                  'egg_2000': null,
                  'egg_2001': null,
                  'egg_2002': null,
                  'egg_2006': null,
                  'egg_2007': null,
                  'egg_2008': null,
                  'egg_2009': null,
                  'egg_2013': null,
                  'egg_2022': null,
                  'egg_2023': null,
                  'egg_2024': null,
                  'egg_2026': null,
                  'egg_2027': null,
                  'egg_2028': null,
                  'egg_2040': null,
                  'egg_2041': null,
                  'egg_2042': null,
                  'egg_2043': null,
                  'egg_2044': null,
                  'egg_2045': null,
                  'egg_2046': null,
                  'egg_2047': null,
                  'egg_2048': null,
                  'egg_2049': null,
                  'egg_2052': null,
                  'egg_2060': null,
                  'egg_2061': null,
                  'egg_2062': null,
                  'egg_2064': null,
                  'egg_2069': null,
                  'egg_2070': null,
                  'egg_2073': null,
                  'egg_2080': null,
                  'egg_2083': null,
                  'egg_2084': null,
                  'egg_2088': null,
                  'egg_2091': null,
                  'egg_2093': null,
                  'egg_2094': null,
                  'egg_2097': null,
                  'egg_2120': null,
                  'egg_2165': null,
                  'egg_2173': null,
                  'egg_2178': null,
                  'egg_2201': null,
                  'egg_2202': null,
                  'egg_2220': null,
                  'egg_2221': null,
                  'egg_2222': null,
                  'egg_2225': null,
                  'egg_2230': null,
                  'egg_2231': null,
                  'egg_2232': null,
                  'egg_2234': null,
                  'egg_2235': null,
                  'egg_2236': null,
                  'egg_2239': null,
                  'egg_2240': null,
                  'egg_2241': null,
                  'egg_2242': null,
                  'egg_2243': null,
                  'egg_2244': null,
                  'egg_2247': null,
                  'egg_2248': null,
                  'egg_2249': null,
                  'egg_2250': null,
                  'egg_3005': null,
                  'egg_3023': null,
                  'egg_3043': null,
                  'egg_3045': null,
                  'egg_3066': null,
                  'egg_3101': null,
                  'egg_3103': null,
                  'egg_3104': null,
                  'egg_3106': null,
                  'egg_3107': null,
                  'egg_3108': null,
                  'egg_3115': null,
                  'egg_3142': null,
                  'egg_3240': null,
                  'egg_3247': null,
                  'egg_4002': null,
                  'egg_4101': null,
                  'egg_4234': null,
                  'egg_4251': null
                };
              }
              var i = 0;
              _.slice(record, 3) // skip field type, unix timestamp, user friendly timestamp columns
              .forEach(function (samplit) {
                if (!writeToFile) {
                  var uint8 = new Buffer(1);
                  uint8[0] = samplit;
                  rowObj[eggIDs[i]] = uint8;  // write as BYTE (all samples < 200) for BigQuery (to save space)
                } else {
                  rowObj[eggIDs[i]] = parseInt(samplit); // write int
                }
                i++;
              });

              // console.log(rowObj);
              rowsForBQ.push(rowObj);
            }
          });

          if (!writeToFile) {
            // max 10,000 rows insert/time on BQ
            let index = 0;
            let size = 6500; // "error": {\n' + "code": 400,\n' +' "message": "Request payload size exceeds the limit: 10485760 bytes.",\n' +
      
            while (index < rowsForBQ.length) {
              let chunk = rowsForBQ.slice(index, size + index);
              await bigquery.dataset(datasetId).table(tableId).insert(chunk);
              //console.log(`Inserted ${chunk.length} rows (max chunker)`);
              index += size;
            }
          } else {
            const replacer = function(key, value) {
              return typeof value === 'undefined' ? null : value;
            } 
            fs.writeFile(`${yyyymmdd}.json`, JSON.stringify(rowsForBQ, replacer), (err) => {
              if (err) throw err;
            });
          }
      });
}

function insert(addDays = 0, startDate = null, writeToFile = false) {
  async function insertAsync(addDays = 0, startDate = null, writeToFile = false) {

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
        { name: 'egg_104', type: 'BYTES' },
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
        { name: 'egg_117', type: 'BYTES' },
        { name: 'egg_118', type: 'BYTES' },
        { name: 'egg_119', type: 'BYTES' },
        { name: 'egg_134', type: 'BYTES' },
        { name: 'egg_142', type: 'BYTES' },
        { name: 'egg_161', type: 'BYTES' },
        { name: 'egg_223', type: 'BYTES' },
        { name: 'egg_224', type: 'BYTES' },
        { name: 'egg_226', type: 'BYTES' },
        { name: 'egg_227', type: 'BYTES' },
        { name: 'egg_228', type: 'BYTES' },
        { name: 'egg_230', type: 'BYTES' },
        { name: 'egg_231', type: 'BYTES' },
        { name: 'egg_233', type: 'BYTES' },
        { name: 'egg_237', type: 'BYTES' },
        { name: 'egg_1000', type: 'BYTES' },
        { name: 'egg_1003', type: 'BYTES' },
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
        { name: 'egg_2244', type: 'BYTES' },
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
        location: 'SG',
        // timePartitioning: {
        //   type: 'DAY',
        //   field: 'recorded_at',
        // },
      };

      await bigquery.dataset(datasetId).createTable(tableId, options);
    } catch {}

    var getDate = moment('1998-08-02', 'YYYY-MM-DD');
    // var getDate = moment('2020-10-20', 'YYYY-MM-DD')

    if (startDate) {
      getDate = moment(startDate, 'YYYY-MM-DD')
    }

    getDate = getDate.add(addDays, 'd');

    request(makeRequestUrl(getDate), function (gcpRequestError, gcpResponse, body) {
      if (handleErrors(gcpRequestError, gcpResponse)) {
          return;
      }

      extractAndInsertEntropy(body, writeToFile);
    });
  }

  insertAsync(addDays, startDate, writeToFile);
}

insert(...process.argv.slice(2));