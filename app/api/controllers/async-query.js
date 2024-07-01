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

// Node packages
const zlib = require('zlib');
const fs = require('fs');

// Queues
var asyncQueue = require('../queues/async-queue');

// Tapis
var tapisV2 = require('vdj-tapis-js/tapis');
var tapisV3 = require('vdj-tapis-js/tapisV3');
var tapisIO = null;
if (config.tapis_version == 2) tapisIO = tapisV2;
if (config.tapis_version == 3) tapisIO = tapisV3;
var tapisSettings = tapisIO.tapisSettings;
var ServiceAccount = tapisIO.serviceAccount;
var GuestAccount = tapisIO.guestAccount;
var authController = tapisIO.authController;
tapisIO.authController.set_config(config);
var webhookIO = require('vdj-tapis-js/webhookIO');

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
AsyncController.asyncQueryRearrangement = function(req, res) {
    var context = "AsyncController.asyncQueryRearrangement";
    config.log.info(context, 'asynchronous query for rearrangements.');

    var bodyData = req.body;
    if (bodyData['facets']) {
        res.status(400).json({"message":"facets not supported."});
        return;
    }

    res.status(500).json({"message":"Not implemented."});

//    req.params.do_async = true;
//    return rearrangementController.queryRearrangements(req, res);
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
