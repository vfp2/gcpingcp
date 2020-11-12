// GCP API - API for Global Consciousness Project
const _ = require('lodash');
const request = require('request');
const csvparser = require('csv-parse');
const moment = require('moment');
const crypto  = require('crypto');
const extractors = require('randomness-extractors');
const fs         = require('fs');

// eg. http://global-mind.org/cgi-bin/eggdatareq.pl?z=1&year=2020&month=1&day=10&stime=00%3A00%3A00&etime=23%3A59%3A59
const API_HOST = "http://global-mind.org";

// http://noosphere.princeton.edu/basket_CSV_v2.html
// Protocol Description: Type 10 Records
// Field 1: Type   Field 2: Item Field 3: Value     Field 4: Comment     Variable
// ...
// 10             4             Trials per sample "Trial size"         $trialsz
// "Trial size" seems fixed at 200 bits/sec so we'll const'ify it
const TRIAL_SIZE = 200;

// The number of seconds it seems to take for most eggs to send their data for near realtime updates
const ESTIMATED_GCP_UPDATE_DELAY_TIME = 0;

// the averaging number of eggs reporting from the past couple of years (as of 2020-01-10)
// seems to be 24-26, we'll be conversative and say 24
const AVG_REPORTING_EGGS = 24;

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

function handleErrors(gcpRequestError, gcpResponse, appResponse) {
    if (gcpRequestError) {
        appResponse.writeHead(200, { 'Content-Type': 'application/json' });
        appResponse.end(JSON.stringify({error: true, type: 'request', message: gcpRequestError}));
        console.error(gcpRequestError);
        return true;
    }

    if (gcpResponse.statusCode != 200) {
        appResponse.writeHead(200, { 'Content-Type': 'application/json' });
        appResponse.end(JSON.stringify({error: true, type: 'status_code', message: gcpResponse.statusCode}));
        console.error("gcpResponse.statusCode != 200: " + gcpResponse.statusCode);
        return true;
    }

    return false;
}

function extractEntropy(requestedSize, appResponse, body, callback) {
    console.log("GCP response size: " + body.length);
    var buffer = Buffer.alloc(requestedSize)
    var offset = 0;
    // parse the csv output
    // ref: http://noosphere.princeton.edu/basket_CSV_v2.html
    csvparser(
        body,
        { relax_column_count: true },
        function (csvParseError, records) {
            if (csvParseError) {
                appResponse.writeHead(200, { 'Content-Type': 'application/json' });
                appResponse.end(JSON.stringify({error: true, type: 'csv_parse', message: csvParseError}));
                console.error(csvParseError);
                return;
            }

            // iterate all lines, process only sample value rows
            var currentByteArr = [];
            records.forEach(function (record) {
                if (record[0] == 13) { // field type 13: actual sample data
                    _.slice(record, 3) // skip field type, unix timestamp, user friendly timestamp columns
                    .forEach(function (samplit) {
                        if (!samplit) {
                            return; // skip void sample values where there was no report from the egg
                        }

                        if (offset == parseInt(requestedSize))
                            return;

                        currentByteArr.push(parseInt(samplit) > TRIAL_SIZE/2 ? 1 : 0);

                        if (currentByteArr.length == 8) {
                            var vonNeumanned = currentByteArr.join('');

                            //https://github.com/ycmjason/randomness-extractors
                            //vonNeumanned = extractors.vonNeumannsExtractor([currentByteArr]);

                            buffer.writeUInt8(parseInt(vonNeumanned, 2), offset++);
                            currentByteArr = [];
                        }
                    });
                }
            });
            callback(createEntropyObject(buffer.slice(0, offset).toString('hex'), requestedSize));
        });
}

//Used to create an entropy object
function createEntropyObject(object_entropy, size) {
    var timestamp = Date.now();
    var entropy = object_entropy.substring(0, size);
    var gid = crypto.createHash('sha256').update(Buffer.from(entropy)).digest('hex');

    var entropyObject = {
        EntropySize: entropy.length,
        Timestamp: timestamp,
        Gid: gid,
        Entropy: entropy
    }

    return entropyObject;
}

exports.getEntropy = function (appRequest, appResponse, callback) {
    var now = moment(moment().utc()).subtract(ESTIMATED_GCP_UPDATE_DELAY_TIME, 'seconds');
    var endTimeFmt = moment(now).format('HH:mm:ss'); // current UTC 00:00:00
    var yearFmt = now.format('YYYY');
    var monthFmt = now.format('MM');
    var dayFmt = now.format('DD');
    console.log("appRequest.query.size: " + appRequest.query.size);
    var requestedSize = appRequest.query.size * 8; // 8 bits for each sample byte (<= 200)
    var secsOfDataNeeded = Math.round(requestedSize / AVG_REPORTING_EGGS) + ESTIMATED_GCP_UPDATE_DELAY_TIME;
    var startTime = moment(now).subtract(secsOfDataNeeded, 'seconds');
    var startTimeFmt = moment(startTime).format('HH:mm:ss') // current UTC 00:00:00 - secsOfDataNeeded
    var includeYesterday = false;
    if (startTime.isBefore(now, 'day')) {
        // if the start time's day yields to be any day before the current day, then as
        // the GCP API only seems to allow single-day queries, we'll set it to 00:00:00
        // and let the logic later on trigger extra API call(s) to get the extra entropy
        startTimeFmt = '00:00:00';

        includeYesterday = true;
    }

    var yesterdayReqUrl = makeRequestUrl(yearFmt, monthFmt, dayFmt - 1, '00:00:00', '23:59:59');
    var todayReqUrl = makeRequestUrl(yearFmt, monthFmt, dayFmt, startTimeFmt, endTimeFmt);

    var writeToFileCallback = function(result) {
        if (result == "1") {
            callback(null, "GID invalid");
        } else {
            fs.writeFile ('./services/entropy/gcp/'+result.Gid+".gcp", JSON.stringify(result), function(err) {
                if (err){
                    callback(JSON.stringify(1));
                } else {
                    console.log('complete');
                    callback(result);
                }
            });
        }
    };

    request(todayReqUrl,
        function (gcpRequestError, gcpResponse, todayBody) {
            if (handleErrors(gcpRequestError, gcpResponse, appResponse)) {
                return;
            }

            if (includeYesterday) {
                // TODO: This is my lazy unfinished work (read hack). If we need more than 1 day's worth of API responses, then just clump all of yesterday's in one go.
                // Sort out proper time filtering at another stage...
                request(yesterdayReqUrl,
                    function (gcpRequestError, gcpResponse, yesterdayBody) {
                        if (handleErrors(gcpRequestError, gcpResponse, appResponse)) {
                            return;
                        }

                        console.log("yesterdayBody + todayBody");
                        extractEntropy(requestedSize, appResponse, yesterdayBody + "\n" + todayBody, writeToFileCallback);
                    });
                return;
            }

            //TODO: Send callback to function -> is really wacky solution, fix later
            extractEntropy(requestedSize, appResponse, todayBody, writeToFileCallback);
    });
}