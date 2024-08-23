'use strict';

//
// async-query.js
// Handle incoming asynchronous query requests
//
// VDJServer Community Data Portal
// ADC API Asynchronous Extension for VDJServer
// https://vdjserver.org
//
// Copyright (C) 2021 The University of Texas Southwestern Medical Center
//
// Author: Scott Christley <scott.christley@utsouthwestern.edu>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.
//

var AsyncController = {};
module.exports = AsyncController;

// App
var app = require('../../app');
var config = require('../../config/config');

// Schema libraries
var airr = require('airr-js');
var vdj_schema = require('vdjserver-schema');

// Node packages
const zlib = require('zlib');
const fs = require('fs');

// Queues
var asyncQueue = require('../queues/async-queue');

// Tapis
var tapisSettings = require('vdj-tapis-js/tapisSettings');
var tapisIO = tapisSettings.get_default_tapis();
var ServiceAccount = tapisIO.serviceAccount;
var GuestAccount = tapisIO.guestAccount;
var authController = tapisIO.authController;
var webhookIO = require('vdj-tapis-js/webhookIO');
var adc_mongo_query = require('vdj-tapis-js/adc_mongo_query');

// return status of asynchronous query
AsyncController.getQueryStatus = function(req, res) {
    var context = "AsyncController.getQueryStatus";
    var uuid = req.params.query_id;

    tapisIO.getAsyncQueryStatus(uuid)
        .then(function(metadata) {
            console.log(metadata);
            if (! metadata) {
                res.status(404).json({"message":"Unknown query identifier."});
                return;
            }
            if (metadata['name'] != 'async_query') {
                res.status(400).json({"message":"Invalid query identifier."});
                return;
            }

            // restrict the info that is sent back
            var entry = {
                query_id: metadata.uuid,
                endpoint: metadata.value.endpoint,
                status: metadata.value.status,
                message: metadata.value.message,
                created: metadata.created,
                estimated_count: metadata.value.estimated_count,
                final_file: metadata.value.final_file,
                download_url: metadata.value.download_url
            };

            res.json(entry);
        })
        .catch(function(error) {
            var msg = config.log.error(context, 'Could not get status.\n.' + error);
            res.status(500).json({"message":msg});
            console.error(msg);
            webhookIO.postToSlack(msg);
        });
}

// submit asynchronous query
AsyncController.asyncQueryRepertoire = function(req, res) {
    var context = "AsyncController.asyncQueryRepertoire";
    config.log.info(context, 'asynchronous query for repertoires.');

    res.status(500).json({"message":"Not implemented."});
}

// submit asynchronous query
AsyncController.asyncQueryRearrangement = async function(req, res) {
    var context = "AsyncController.asyncQueryRearrangement";
    var msg = null;

    config.log.info(context, 'asynchronous query for rearrangements.');

    // 1. process the query for basic validation
    // 2. create async meta record
    // 3. submit job to queue

    var bodyData = req.body;
    if (bodyData['facets']) {
        res.status(400).json({"message":"facets not supported."});
        return;
    }

    // check max query size
    var bodyLength = JSON.stringify(bodyData).length;
    if (bodyLength > config.info.max_query_size) {
        msg = "Query size (" + bodyLength + ") exceeds maximum size of " + config.info.max_query_size + " characters.";
        res.status(400).json({"message":msg});
        msg = config.log.error(context, msg);
        webhookIO.postToSlack(msg);
        return;
    }

    // async queries have a different max
    // if the query does not specify a size, we need to count to see if the
    // result set is too big. we indicate this with a null size.
    var size = null;
    if (bodyData['size'] != undefined) {
        size = bodyData['size'];
        if (size > config.async.max_size) {
            msg = "Size too large (" + size + "), maximum size is " + config.async.max_size;
            res.status(400).json({"message":msg});
            msg = config.log.error(context, msg);
            webhookIO.postToSlack(msg);
            return;
        }
    }

    // AIRR fields
    var all_fields = [];
    var airr_schema = airr.get_schema('Rearrangement')['definition'];
    if (bodyData['include_fields']) {
        airr.collectFields(airr_schema, bodyData['include_fields'], all_fields, null);
    }
    // collect all AIRR schema fields
    var schema_fields = [];
    airr.collectFields(airr_schema, 'airr-schema', schema_fields, null);

    // field projection
    var projection = {};
    if (bodyData['fields'] != undefined) {
        var fields = bodyData['fields'];
        //if (config.debug) console.log('fields: ', fields);
        if (! (fields instanceof Array)) {
            msg = "fields parameter is not an array.";
            res.status(400).json({"message":msg});
            return;
        }
        for (let i = 0; i < fields.length; ++i) {
            if (fields[i] == '_id') continue;
            if (fields[i] == '_etag') continue;
            projection[fields[i]] = 1;
        }
        projection['_id'] = 1;

        // add AIRR required fields to projection
        // NOTE: projection will not add a field if it is not already in the document
        // so below after the data has been retrieved, missing fields need to be
        // added with null values.
        if (all_fields.length > 0) {
            for (var r in all_fields) projection[all_fields[r]] = 1;
        }

        // add to field list so will be put in response if necessary
        for (let i = 0; i < fields.length; ++i) {
            if (fields[i] == '_id') continue;
            all_fields.push(fields[i]);
        }
    }

    // format parameter
    var format = 'json';
    if (bodyData['format'] != undefined)
        format = bodyData['format'];
    if ((format != 'json') && (format != 'tsv')) {
        msg = "Unsupported format (" + format + ").";
        res.status(400).json({"message":msg});
        return;
    }

    // construct query string
    var filter = {};
    var query = undefined;
    if (bodyData['filters'] != undefined) {
        filter = bodyData['filters'];
        try {
            var error = { message: '' };
            query = adc_mongo_query.constructQueryOperation(airr, airr_schema, filter, error, false, true);
            //console.log(query);

            if (!query) {
                msg = "Could not construct valid query. Error: " + error['message'];
                res.status(400).json({"message":msg});
                msg = config.log.error(context, msg);
                webhookIO.postToSlack(msg);
                return;
            }
        } catch (e) {
            msg = "Could not construct valid query: " + e;
            res.status(400).json({"message":msg});
            msg = config.log.error(context, msg);
            webhookIO.postToSlack(msg);
            return;
        }
    }

    // eliminate any extra fields from the query
    // TODO: hard-coded so should we look at schema instead?
    var trimBody = {};
    for (let p in bodyData) {
        switch(p) {
            case 'filters':
            case 'format':
            case 'fields':
            case 'size':
            case 'from':
            case 'include_fields':
            case 'notification':
                trimBody[p] = bodyData[p];
        }
    }

    // create metadata entry
    var collection = 'rearrangement' + tapisSettings.mongo_queryCollection;
    var metadata = await tapisIO.createAsyncQueryMetadata('rearrangement', collection, trimBody)
        .catch(function(error) {
            msg = 'tapisIO.createAsyncQueryMetadata, internal service error: ' + error;
        });
    if (msg) {
            res.status(500).json({"message":msg});
            msg = config.log.error(context, msg);
            webhookIO.postToSlack(msg);
            return;
    }

    config.log.info(context, 'Created async metadata:', metadata.uuid);
    res.status(200).json({"message":"rearrangement async query accepted.", "query_id": metadata.uuid});

    // trigger the queue
    asyncQueue.triggerQueue();

    return;
}

// submit asynchronous query
AsyncController.asyncQueryClone = function(req, res) {
    var context = "AsyncController.asyncQueryClone";
    config.log.info(context, 'asynchronous query for clones.');

    res.status(500).json({"message":"Not implemented."});
}

// When a count aggregation is performed, the output is simple record with the number
// This function reads and parsed the file.
async function readCountFile(filename) {

    return new Promise((resolve, reject) => {
        const rd = fs.readFileSync(filename);
        try {
            var obj = JSON.parse(rd);
            resolve(obj);
        } catch (e) {
            reject(e);
        }
    })
}

// receive notification from Tapis LRQ
AsyncController.asyncNotify = async function(req, res) {
    var context = 'AsyncController.asyncNotify';
    config.log.info(context, 'Received LRQ notification id:', req.params.notify_id, 'body:', JSON.stringify(req.body));

    // return a response
    res.status(200).json({"message":"notification received."});

    // TODO: do we need this anymore?
}

/*
// Bull queues
var submitQueue = new Queue('lrq submit');
var finishQueue = new Queue('lrq finish');

// return status of asynchronous query
AsyncController.getQueryStatus = function(req, res) {
    var uuid = req.params.query_id;

    agaveIO.getMetadata(uuid)
        .then(function(metadata) {
            console.log(metadata);
            if (! metadata) {
                res.status(404).json({"message":"Unknown query identifier."});
                return;
            }
            if (metadata['name'] != 'async_query') {
                res.status(400).json({"message":"Invalid query identifier."});
                return;
            }

            // restrict the info that is sent back
            var entry = {
                query_id: metadata.uuid,
                endpoint: metadata.value.endpoint,
                status: metadata.value.status,
                message: metadata.value.message,
                created: metadata.created,
                estimated_count: metadata.value.estimated_count,
                final_file: metadata.value.final_file,
                download_url: metadata.value.download_url
            };

            res.json(entry);
        })
        .catch(function(error) {
            var msg = 'VDJ-ADC-API ERROR (getStatus): Could not get status.\n.' + error;
            res.status(500).json({"message":"Internal service error."});
            console.error(msg);
            //webhookIO.postToSlack(msg);
        });
}

// submit asynchronous query
AsyncController.asyncQueryRepertoire = function(req, res) {
    if (config.debug) console.log('VDJ-ADC-API INFO: asynchronous query for repertoires.');

    res.status(500).json({"message":"Not implemented."});
}

// submit asynchronous query
AsyncController.asyncQueryRearrangement = function(req, res) {
    if (config.debug) console.log('VDJ-ADC-API INFO: asynchronous query for rearrangements.');

    var bodyData = req.body;
    if (bodyData['facets']) {
        res.status(400).json({"message":"facets not supported."});
        return;
    }

    req.params.do_async = true;
    return rearrangementController.queryRearrangements(req, res);
}

// submit asynchronous query
AsyncController.asyncQueryClone = function(req, res) {
    if (config.debug) console.log('VDJ-ADC-API INFO: asynchronous query for clones.');

    res.status(500).json({"message":"Not implemented."});
}

// When a count aggregation is performed, the output is simple record with the number
// This function reads and parsed the file.
async function readCountFile(filename) {

    return new Promise((resolve, reject) => {
        const rd = fs.readFileSync(filename);
        try {
            var obj = JSON.parse(rd);
            resolve(obj);
        } catch (e) {
            reject(e);
        }
    })
}

// receive notification from Tapis LRQ
AsyncController.asyncNotify = async function(req, res) {
    var context = 'AsyncController.asyncNotify';
    console.log('VDJ-ADC-API-ASYNC INFO: Received LRQ notification id:', req.params.notify_id, 'body:', JSON.stringify(req.body));

    // return a response
    res.status(200).json({"message":"notification received."});

    // search for metadata item based on LRQ id
    var msg = null;
    var lrq_id = req.body['result']['_id']
    console.log(lrq_id);
    var metadata = await agaveIO.getAsyncQueryMetadata(lrq_id)
        .catch(function(error) {
            msg = 'VDJ-ADC-ASYNC-API ERROR (asyncNotify): Could not get metadata for LRG id: ' + lrq_id + ', error: ' + error;
            console.error(msg);
            webhookIO.postToSlack(msg);
            return Promise.reject(new Error(msg));
        });

    // do some error checking
    console.log(metadata);
    if (metadata.length != 1) {
        msg = 'VDJ-ADC-ASYNC-API ERROR (asyncNotify): Expected single metadata entry but got ' + metadata.length + ' for LRG id: ' + lrq_id;
        console.error(msg);
        webhookIO.postToSlack(msg);
        return Promise.reject(new Error(msg));
    }
    metadata = metadata[0];
    if (metadata['uuid'] != req.params.notify_id) {
        msg = 'Notification id and LRQ id do not match: ' + req.params.notify_id + ' != ' + metadata['uuid'];
        console.error(msg);
        webhookIO.postToSlack(msg);
        return Promise.reject(new Error(msg));
    }

    if (metadata['value']['status'] == 'COUNTING') {
        // if this is a count query
        // get the count
        var filename = config.lrqdata_path + 'lrq-' + metadata["value"]["lrq_id"] + '.json';
        var countFail = false;
        var count_obj = await readCountFile(filename)
            .catch(function(error) {
                msg = 'VDJ-ADC-ASYNC-API ERROR (asyncNotify): Could not read count file (' + filename + ') for LRQ ' + metadata["uuid"] + '.\n' + error;
                console.error(msg);
                webhookIO.postToSlack(msg);
                countFail = true;
                console.log(countFail);
                console.log(metadata);
                //return Promise.reject(new Error(msg));
            });
        if (countFail) {
            config.log.info(context, 'Sleep and retry to read count file.');
            await new Promise(resolve => setTimeout(resolve, 60000));
            countFail = false;
            count_obj = await readCountFile(filename)
                .catch(function(error) {
                    msg = 'VDJ-ADC-ASYNC-API ERROR (asyncNotify): Could not read count file (' + filename + ') for LRQ ' + metadata["uuid"] + '.\n' + error;
                    console.error(msg);
                    webhookIO.postToSlack(msg);
                    countFail = true;
                });
        }
        console.log('fall through');
        console.log(metadata);
        console.log(countFail);
        console.log(count_obj);

        // error if the count is greater than max size
        if (countFail || (count_obj['total_records'] > config.async.max_size)) {
            console.log('got here');
            metadata['value']['status'] = 'ERROR';
            if (countFail) {
                metadata['value']['message'] = 'Could not read count file';
            } else {
                metadata['value']['estimated_count'] = count_obj['total_records'];
                metadata['value']['message'] = 'Result size (' + count_obj['total_records'] + ') is larger than maximum size (' + config.async.max_size + ')';
            }
            msg = 'VDJ-ADC-ASYNC-API ERROR (asyncNotify): Query rejected: ' + metadata["uuid"] + ', ' + metadata['value']['message'];
            console.error(msg);
            webhookIO.postToSlack(msg);

            await agaveIO.updateMetadata(metadata['uuid'], metadata['name'], metadata['value'], null)
                .catch(function(error) {
                    msg = 'VDJ-ADC-ASYNC-API ERROR (asyncNotify): Could not update metadata for LRQ ' + metadata["uuid"] + '.\n' + error;
                    console.error(msg);
                    webhookIO.postToSlack(msg);
                    return Promise.reject(new Error(msg));
                });

            if (metadata["value"]["notification"]) {
                var notify = asyncQueue.checkNotification(metadata);
                if (notify) {
                    var data = asyncQueue.cleanStatus(metadata);
                    await agaveIO.sendNotification(notify, data)
                        .catch(function(error) {
                            var cmsg = 'VDJ-ADC-ASYNC-API ERROR (countQueue): Could not post notification.\n' + error;
                            console.error(cmsg);
                            webhookIO.postToSlack(cmsg);
                        });
                }
            }
            return Promise.resolve();
        }

        // update metadata status
        metadata['value']['estimated_count'] = count_obj['total_records'];
        metadata['value']['count_lrq_id'] = metadata['value']['lrq_id'];
        metadata['value']['status'] = 'COUNTED';
        await agaveIO.updateMetadata(metadata['uuid'], metadata['name'], metadata['value'], null)
            .catch(function(error) {
                msg = 'VDJ-ADC-ASYNC-API ERROR (asyncNotify): Could not update metadata for LRQ ' + metadata["uuid"] + '.\n' + error;
                console.error(msg);
                webhookIO.postToSlack(msg);
                return Promise.reject(new Error(msg));
            });

        // otherwise submit the real query
        submitQueue.add({metadata: metadata});

        return Promise.resolve();

    } else {
        if (req.body['status'] == 'FINISHED') {
            metadata['value']['status'] = 'PROCESSING';
            metadata['value']['raw_file'] = req.body['result']['location'];
        } else {
            // TODO: what else besides FINISHED?
            metadata['value']['status'] = req.body['status'];
        }

        // update with additional info
        // TODO: should we retry on error?
        var new_metadata = await agaveIO.updateMetadata(metadata['uuid'], metadata['name'], metadata['value'], null)
            .catch(function(error) {
                msg = 'VDJ-ADC-ASYNC-API ERROR (countQueue): Could not update metadata for LRQ ' + metadata["uuid"] + '.\n' + error;
                console.error(msg);
                webhookIO.postToSlack(msg);
            });

        if (new_metadata) {
            // submit queue job to finish processing
            finishQueue.add({metadata: new_metadata});
        }
    }

    return Promise.resolve();
}
*/
