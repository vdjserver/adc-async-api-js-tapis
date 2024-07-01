'use strict';

//
// async-queue.js
// Job queue for processing asynchronous query requests
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

var AsyncQueue = {};
module.exports = AsyncQueue;

// App
var app = require('../../app');
var config = require('../../config/config');

// Node packages
const fs = require('fs');
const fsPromises = require('fs').promises;
var Queue = require('bull');

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


AsyncQueue.cleanStatus = function(metadata) {
    var entry = {
        query_id: metadata.uuid,
        endpoint: metadata.value.endpoint,
        status: metadata.value.status,
        message: metadata.value.message,
        created: metadata.created,
        final_file: metadata.value.final_file,
        download_url: metadata.value.download_url
    };
    return entry;
}

// check if should send notification for status
AsyncQueue.checkNotification = function(metadata) {
    var notify = metadata["value"]["notification"];
    if (notify["events"]) {
        if (notify["events"].indexOf(metadata["value"]["status"]) < 0)
            notify = null;
    }
    return notify;
}

/*
// Steps for a long-running query
// 1. Process request parameters, construct query
// 2. Submit query to Tapis LRQ API
// 3. Create metadata record with any additional info
// ... wait for notification that query is done
// 4. Additional processing/formating of the data, move file?
// 5. Update metadata with status
// 6. Send notification

AsyncQueue.processQueryJobs = function() {
    var context = 'AsyncQueue.processQueryJobs';
    var countQueue = new Queue('lrq count');
    var submitQueue = new Queue('lrq submit');
    var finishQueue = new Queue('lrq finish');

    countQueue.process(async (job) => {
        // If we do not know the size of the result set, which we generally do not unless
        // the query specifies a size, we first perform a count. The query controller
        // defines count_aggr to generate the count.

        var context = 'countQueue.process';
        var msg = null;
        var metadata = job['data']['metadata'];
        config.log.info(context, 'submitting count aggregation for LRQ:', metadata['uuid']);
        //console.log(job['data']);

        var controller = null;
        if (metadata["value"]["endpoint"] == "repertoire") controller = repertoireController;
        if (metadata["value"]["endpoint"] == "rearrangement") controller = rearrangementController;
        if (! controller) {
            msg = config.log.error(context, 'Unknown endpoint: ' + metadata["value"]["endpoint"]);
            return Promise.reject(new Error(msg));
        }

        // submit the count aggregation query
        var notification = agaveSettings.notifyHost + '/airr/async/v1/notify/' + metadata['uuid'];
        var count_aggr = controller.generateAsyncCountQuery(metadata);
        //console.log(JSON.stringify(count_aggr));
        var async_query = await agaveIO.performAsyncAggregation('count_query', metadata['value']['collection'], count_aggr, notification)
            .catch(function(error) {
                msg = config.log.error(context, 'Could not submit count query for LRQ ' + metadata['uuid'] + '.\n' + error);
                webhookIO.postToSlack(msg);
            });

        // set to error status
        if (! async_query) {
            metadata["value"]["status"] = "ERROR";
            metadata["value"]["message"] = msg;
            await agaveIO.updateMetadata(metadata['uuid'], metadata['name'], metadata['value'], null);

            if (metadata["value"]["notification"]) {
                var notify = AsyncQueue.checkNotification(metadata);
                if (notify) {
                    var data = AsyncQueue.cleanStatus(metadata);
                    await agaveIO.sendNotification(notify, data)
                        .catch(function(error) {
                            var cmsg = config.log.error(context, 'Could not post notification.\n' + error);
                            webhookIO.postToSlack(cmsg);
                        });
                }
            }

            return Promise.reject(new Error(msg));
        }

        config.log.info(context, 'Count aggregation submitted with LRQ ID:', async_query['_id']);

        // update metadata
        metadata['value']['lrq_id'] = async_query['_id'];
        metadata['value']['status'] = 'COUNTING';
        await agaveIO.updateMetadata(metadata['uuid'], metadata['name'], metadata['value'], null)
            .catch(function(error) {
                msg = config.log.error(context, 'Could not update metadata for LRQ ' + metadata["uuid"] + '.\n' + error);
                webhookIO.postToSlack(msg);
            });

        return Promise.resolve();
    });

    submitQueue.process(async (job) => {
        // submit query LRQ API

        var context = 'submitQueue.process';
        var msg = null;
        var metadata = job['data']['metadata'];
        config.log.info(context, 'submitting query for LRQ:', metadata['uuid']);
        //console.log(job['data']);

        var controller = null;
        if (metadata["value"]["endpoint"] == "repertoire") controller = repertoireController;
        if (metadata["value"]["endpoint"] == "rearrangement") controller = rearrangementController;
        if (! controller) {
            msg = config.log.error(context, 'Unknown endpoint: ' + metadata["value"]["endpoint"]);
            return Promise.reject(new Error(msg));
        }

        // submit the full query
        var notification = agaveSettings.notifyHost + '/airr/async/v1/notify/' + metadata['uuid'];
        var async_query = null;
        var query_aggr = controller.generateAsyncQuery(metadata);
        //console.log(JSON.stringify(query_aggr));
        if (query_aggr.length == 1) {
            // if only one entry then it is a simple query
            async_query = await agaveIO.performAsyncQuery(metadata['value']['collection'], query_aggr[0]["$match"], null, notification)
                .catch(function(error) {
                    msg = config.log.error(context, 'Could not submit full query for LRQ ' + metadata['uuid'] + '.\n.' + error);
                    webhookIO.postToSlack(msg);
                });
        } else {
            async_query = await agaveIO.performAsyncAggregation('full_query', metadata['value']['collection'], query_aggr, notification)
                .catch(function(error) {
                    msg = config.log.error(context, 'Could not submit full query for LRQ ' + metadata['uuid'] + '.\n.' + error);
                    webhookIO.postToSlack(msg);
                });
        }

        // set to error status if failed
        if (! async_query) {
            metadata["value"]["status"] = "ERROR";
            await agaveIO.updateMetadata(metadata['uuid'], metadata['name'], metadata['value'], null);

            if (metadata["value"]["notification"]) {
                let notify = AsyncQueue.checkNotification(metadata);
                if (notify) {
                    let data = AsyncQueue.cleanStatus(metadata);
                    await agaveIO.sendNotification(notify, data)
                        .catch(function(error) {
                            var cmsg = config.log.error(context, 'Could not post notification.\n' + error);
                            webhookIO.postToSlack(cmsg);
                        });
                }
            }

            return Promise.reject(new Error(msg));
        }

        config.log.info(context, 'Full query submitted with LRQ ID:', async_query['_id']);

        // update metadata
        metadata['value']['lrq_id'] = async_query['_id'];
        metadata['value']['status'] = 'SUBMITTED';
        await agaveIO.updateMetadata(metadata['uuid'], metadata['name'], metadata['value'], null)
            .catch(function(error) {
                msg = config.log.error(context, 'Could not update metadata for LRQ ' + metadata["uuid"] + '.\n' + error);
                webhookIO.postToSlack(msg);
            });

        if (metadata["value"]["notification"]) {
            let notify = AsyncQueue.checkNotification(metadata);
            if (notify) {
                let data = AsyncQueue.cleanStatus(metadata);
                await agaveIO.sendNotification(notify, data)
                    .catch(function(error) {
                        var cmsg = config.log.error(context, 'Could not post notification.\n' + error);
                        webhookIO.postToSlack(cmsg);
                    });
            }
        }

        return Promise.resolve();
    });

    finishQueue.process(async (job) => {
        // process data
        var context = 'finishQueue.process';
        var msg = null;
        var metadata = job['data']['metadata'];

        var controller = null;
        if (metadata["value"]["endpoint"] == "repertoire") controller = repertoireController;
        if (metadata["value"]["endpoint"] == "rearrangement") controller = rearrangementController;
        if (! controller) {
            msg = config.log.error(context, 'Unknown endpoint: ' + metadata["value"]["endpoint"]);
            return Promise.reject(new Error(msg));
        }

        // process data into final format
        var outname = await controller.processLRQfile(metadata["uuid"])
            .catch(function(error) {
                msg = config.log.error(context, 'Could not finish processing LRQ ' + metadata["uuid"] + '.\n' + error);
                webhookIO.postToSlack(msg);
            });

        // set to error status
        if (! outname) {
            metadata["value"]["status"] = "ERROR";
            await agaveIO.updateMetadata(metadata['uuid'], metadata['name'], metadata['value'], null);

            if (metadata["value"]["notification"]) {
                let notify = AsyncQueue.checkNotification(metadata);
                if (notify) {
                    let data = AsyncQueue.cleanStatus(metadata);
                    await agaveIO.sendNotification(notify, data)
                        .catch(function(error) {
                            var cmsg = config.log.error(context, 'Could not post notification.\n' + error);
                            webhookIO.postToSlack(cmsg);
                        });
                }
            }

            return Promise.reject(new Error(msg));
        }

        config.log.info(context, 'final processed file: ' + outname);
        metadata["value"]["final_file"] = outname;

        // create postit with expiration
        // TODO: How to handle permanent?
        var url = 'https://' + agaveSettings.hostname
            + '/files/v2/media/system/'
            + agaveSettings.storageSystem
            + '//irplus/data/lrqdata/' + outname
            + '?force=true';

        var postit = await agaveIO.createPublicFilePostit(url, false, config.async.max_uses, config.async.lifetime)
            .catch(function(error) {
                msg = config.log.error(context, 'Could not create postit for LRQ ' + metadata["uuid"] + '.\n' + error);
                webhookIO.postToSlack(msg);
            });

        // set to error status
        if (! postit) {
            metadata["value"]["status"] = "ERROR";
            await agaveIO.updateMetadata(metadata['uuid'], metadata['name'], metadata['value'], null);

            if (metadata["value"]["notification"]) {
                let notify = AsyncQueue.checkNotification(metadata);
                if (notify) {
                    let data = AsyncQueue.cleanStatus(metadata);
                    await agaveIO.sendNotification(notify, data)
                        .catch(function(error) {
                            let cmsg = config.log.error(context, 'Could not post notification.\n' + error);
                            webhookIO.postToSlack(cmsg);
                        });
                }
            }

            return Promise.reject(new Error(msg));
        }

        // update with processed file
        config.log.info(context, 'Created postit: ' + postit["postit"]);
        metadata["value"]["postit_id"] = postit["postit"];
        metadata["value"]["download_url"] = postit["_links"]["self"]["href"];
        metadata["value"]["status"] = "FINISHED";
        var retry = false;
        await agaveIO.updateMetadata(metadata['uuid'], metadata['name'], metadata['value'], null)
            .catch(function(error) {
                msg = config.log.error(context, 'Could not update metadata for LRQ ' + metadata["uuid"] + '.\n' + error);
                retry = true;
            });
        if (retry) {
            config.log.info(context, 'Retrying updateMetadata');
            await agaveIO.updateMetadata(metadata['uuid'], metadata['name'], metadata['value'], null)
            .catch(function(error) {
                msg = config.log.error(context, 'Could not update metadata for LRQ ' + metadata["uuid"] + '. Metadata in inconsistent state.\n' + error);
                webhookIO.postToSlack(msg);
                return Promise.reject(new Error(msg));
            });
        }

        // send notification
        if (metadata["value"]["notification"]) {
            let notify = AsyncQueue.checkNotification(metadata);
            if (notify) {
                let data = AsyncQueue.cleanStatus(metadata);
                await agaveIO.sendNotification(notify, data)
                    .catch(function(error) {
                        let cmsg = config.log.error(context, 'Could not post notification.\n' + error);
                        webhookIO.postToSlack(cmsg);
                    });
            }
        }

        return Promise.resolve();
    });
}

// Sadly we need our own polling mechanism for LRQ
// because we cannot trust their notifications
var pollQueue = new Queue('ADC ASYNC polling');
AsyncQueue.triggerPolling = async function() {
    var context = 'AsyncQueue.triggerPolling';
    var msg = null;

    if (! config.async.enable_poll) {
        msg = 'Polling is not enabled in configuration, cannot trigger';
        config.log.info(context, msg);
        //msg = config.log.error(context, 'Polling is not enabled in configuration, cannot trigger');
        //webhookIO.postToSlack(msg);
        return Promise.reject(new Error(msg));
    }

    config.log.info(context, 'start');

    // Check if any open COUNTING queries
    var counts = await agaveIO.getAsyncQueryMetadataWithStatus('COUNTING')
        .catch(function(error) {
            msg = config.log.error(context, 'Could not get COUNTING metadata.\n' + error);
            webhookIO.postToSlack(msg);
            return Promise.reject(new Error(msg));
        });

    config.log.info(context, 'Found', counts.length, 'records with COUNTING status.');
    //console.log(counts);

    // Check if any open SUBMITTED queries
    var submits = await agaveIO.getAsyncQueryMetadataWithStatus('SUBMITTED')
        .catch(function(error) {
            msg = config.log.error(context, 'Could not get SUBMITTED metadata.\n' + error);
            webhookIO.postToSlack(msg);
            return Promise.reject(new Error(msg));
        });

    config.log.info(context, 'Found', submits.length, 'records with SUBMITTED status.');
    //console.log(submits);

    // check every 600secs/10mins
    pollQueue.add({}, { repeat: { every: 600000 }});

    // testing, every 60 secs
    //pollQueue.add({}, { repeat: { every: 60000 }});

    config.log.info(context, 'end');
}

// Check for async queries where the LRQ is FINISHED
// but we have not received the notification.

pollQueue.process(async (job) => {
    var context = 'pollQueue.process';
    var msg = null;

    if (! config.async.enable_poll) {
        msg = 'Polling is not enabled in configuration, cannot trigger';
        config.log.info(context, msg);
        //msg = config.log.error(context, 'Polling is not enabled in configuration, exiting.');
        //webhookIO.postToSlack(msg);
        return Promise.resolve();
    }

    config.log.info(context, 'Checking for entries.');

    // Check if any open COUNTING queries
    var counts = await agaveIO.getAsyncQueryMetadataWithStatus('COUNTING')
        .catch(function(error) {
            msg = config.log.error(context, 'Could not get COUNTING metadata.\n' + error);
            webhookIO.postToSlack(msg);
            return Promise.reject(new Error(msg));
        });

    config.log.info(context, 'Found', counts.length, 'records with COUNTING status.');

    if (counts.length > 0) {
        for (let i in counts) {
            let entry = counts[i];
            //console.log(entry);

            if (! entry['value']['lrq_id']) {
                config.log.info(context, 'Entry', entry['uuid'], 'is missing lrq_id, skipping.');
                continue;
            }

            let lrq_status = await agaveIO.getLRQStatus(entry['value']['lrq_id'])
                .catch(function(error) {
                    msg = config.log.error(context, 'Could not get LRQ status of ' + entry['value']['lrq_id'] + ' for metadata ' + entry['uuid'] + '.\n.' + error);
                    webhookIO.postToSlack(msg);
                });

            //console.log(lrq_status);

            if (lrq_status.status == 'FINISHED') {
                if (lrq_status.notification) {
                    // found one! manually post the notification, hack the POST data
                    config.log.info(context, 'Manually posting notification for', entry['uuid']);

                    let filename = 'lrq-' + entry["value"]["lrq_id"] + '.json';
                    let data = {
                        result: {
                            location: "https://vdj-agave-api.tacc.utexas.edu/files/v2/media/system/data.vdjserver.org//irplus/data/lrqdata/" + filename,
                            _id: entry["value"]["lrq_id"]
                        },
                        status: "FINISHED",
                        message: "notification manually sent by pollQueue"
                    };

                    await agaveIO.sendNotification({url: lrq_status.notification, method: 'POST'}, data)
                        .catch(function(error) {
                            msg = config.log.error(context, 'Could not post notification.\n' + error);
                            webhookIO.postToSlack(msg);
                            return Promise.reject(new Error(msg));
                        });
                }
            }
        }
    }

    // Check if any open SUBMITTED queries
    var submits = await agaveIO.getAsyncQueryMetadataWithStatus('SUBMITTED')
        .catch(function(error) {
            msg = config.log.error(context, 'Could not get SUBMITTED metadata.\n' + error);
            webhookIO.postToSlack(msg);
            return Promise.reject(new Error(msg));
        });

    config.log.info(context, 'Found', submits.length, 'records with SUBMITTED status.');

    if (submits.length > 0) {
        for (let i in submits) {
            let entry = submits[i];
            //console.log(entry);

            if (! entry['value']['lrq_id']) {
                config.log.info(context, 'Entry', entry['uuid'], 'is missing lrq_id, skipping.');
                continue;
            }

            let lrq_status = await agaveIO.getLRQStatus(entry['value']['lrq_id'])
                .catch(function(error) {
                    msg = config.log.error(context, 'Could not get LRQ status of ' + entry['value']['lrq_id'] + ' for metadata ' + entry['uuid'] + '.\n.' + error);
                    webhookIO.postToSlack(msg);
                });

            //console.log(lrq_status);

            if (lrq_status.status == 'FINISHED') {
                if (lrq_status.notification) {
                    // found one! manually post the notification, hack the POST data
                    config.log.info(context, 'Manually posting notification for', entry['uuid']);

                    let filename = 'lrq-' + entry["value"]["lrq_id"] + '.json';
                    let data = {
                        result: {
                            location: "https://vdj-agave-api.tacc.utexas.edu/files/v2/media/system/data.vdjserver.org//irplus/data/lrqdata/" + filename,
                            _id: entry["value"]["lrq_id"]
                        },
                        status: "FINISHED",
                        message: "notification manually sent by pollQueue"
                    };

                    await agaveIO.sendNotification({url: lrq_status.notification, method: 'POST'}, data)
                        .catch(function(error) {
                            msg = config.log.error(context, 'Could not post notification.\n' + error);
                            webhookIO.postToSlack(msg);
                            return Promise.reject(new Error(msg));
                        });

                    // only trigger one, so the processing code does not get overloaded
                    // if there are more, they will get triggered when the poll job repeats
                    return Promise.resolve();
                }
            }
        }
    }

    return Promise.resolve();
});

// check if any queries need to be expired
var expireQueue = new Queue('ADC ASYNC expire');
AsyncQueue.triggerExpiration = async function() {
    var context = 'AsyncQueue.triggerExpiration';
    var msg = null;

    if (! config.async.enable_expire) {
        msg = 'Expiration is not enabled in configuration, cannot trigger';
        config.log.info(context, msg);
        //msg = config.log.error(context, 'Expiration is not enabled in configuration, cannot trigger');
        //webhookIO.postToSlack(msg);
        return Promise.resolve();
    }

    config.log.info(context, 'start');

    // submit to check every 3600secs/1hour
    expireQueue.add({}, { repeat: { every: 3600000 }});

    // testing, every 2 mins
    //expireQueue.add({}, { repeat: { every: 120000 }});

    config.log.info(context, 'end');
}

// Check for async queries where the postit lifetime
// has expired, thus data can no longer be downloaded

expireQueue.process(async (job) => {
    var context = 'expireQueue.process';
    var msg = null;

    if (! config.async.enable_expire) {
        msg = 'Expiration is not enabled in configuration, cannot trigger';
        config.log.info(context, msg);
        //msg = config.log.error(context, 'Expiration is not enabled in configuration, cannot trigger');
        //webhookIO.postToSlack(msg);
        return Promise.resolve();
    }

    config.log.info(context, 'Checking for entries.');

    // Get all FINISHED queries
    var finish = await agaveIO.getAsyncQueryMetadataWithStatus('FINISHED')
        .catch(function(error) {
            msg = config.log.error(context, 'Could not get FINISHED metadata.\n' + error);
            webhookIO.postToSlack(msg);
            return Promise.reject(new Error(msg));
        });

    config.log.info(context, 'Found', finish.length, 'records with FINISHED status.');

    if (finish.length > 0) {
        for (var i in finish) {
            msg = null;
            var shouldExpire = false;
            var metadata = finish[i];
            //console.log(metadata);

            // if missing postit for some reason, expire it
            if (! metadata['value']['postit_id']) {
                config.log.info(context, 'Entry', metadata['uuid'], 'is missing postit_id, expiring.');
                shouldExpire = true;
            } else {
                // get postit
                var postit = await agaveIO.getPostit(metadata['value']['postit_id'])
                    .catch(function(error) {
                        msg = config.log.error(context, 'Could not get postit: ' + metadata['value']['postit_id'] + '.\n' + error);
                        webhookIO.postToSlack(msg);
                        return Promise.reject(new Error(msg));
                    });
                //console.log(postit);

                // check if it has expired
                if (postit['status'] == 'EXPIRED') shouldExpire = true;

                // TODO: we should check if postit is expired, but cannot get, Tapis bug
                // TODO: instead compare against lifetime
                //var create_date = new Date(metadata['created']);
                //var now = Date.now();
                //var diff = now - create_date;
                //console.log(create_date, now, diff);

                // check if it has expired
                //if (diff > (config.async.lifetime * 1000)) shouldExpire = true;
            }

            if (shouldExpire) {
                config.log.info(context, 'Expiring entry:', metadata['uuid']);

                // delete LRQ count file
                if (metadata['value']['count_lrq_id']) {
                    var thefile = config.lrqdata_path + 'lrq-' + metadata["value"]["count_lrq_id"] + '.json';
                    try {
                        await fsPromises.unlink(thefile);
                    } catch (e) {
                        // ignore if file does not exist
                        if (e.code != 'ENOENT') {
                            msg = config.log.error(context, 'Unknown error deleting ' + thefile + ', error: ' + e);
                            webhookIO.postToSlack(msg);
                        }
                    }
                }

                // delete LRQ data file
                if (metadata['value']['lrq_id']) {
                    var thefile = config.lrqdata_path + 'lrq-' + metadata["value"]["lrq_id"] + '.json';
                    try {
                        await fsPromises.unlink(thefile);
                    } catch (e) {
                        // ignore if file does not exist
                        if (e.code != 'ENOENT') {
                            msg = config.log.error(context, 'Unknown error deleting ' + thefile + ', error: ' + e);
                            webhookIO.postToSlack(msg);
                        }
                    }
                }

                // delete final file
                if (metadata['value']['final_file']) {
                    var thefile = config.lrqdata_path + metadata["value"]["final_file"];
                    try {
                        await fsPromises.unlink(thefile);
                    } catch (e) {
                        if (e.code != 'ENOENT') {
                            msg = config.log.error(context, 'Unknown error deleting ' + thefile + ', error: ' + e);
                            webhookIO.postToSlack(msg);
                        }
                    }
                }

                // update metadata if no errors
                if (!msg) {
                    metadata['value']['status'] = 'EXPIRED';
                    await agaveIO.updateMetadata(metadata['uuid'], metadata['name'], metadata['value'], null)
                        .catch(function(error) {
                            msg = config.log.error(context, 'Could not update metadata for LRQ ' + metadata["uuid"] + '.\n' + error);
                            webhookIO.postToSlack(msg);
                        });
                }

                // send notification
                if (metadata["value"]["notification"]) {
                    let notify = AsyncQueue.checkNotification(metadata);
                    if (notify) {
                        let data = AsyncQueue.cleanStatus(metadata);
                        await agaveIO.sendNotification(notify, data)
                            .catch(function(error) {
                                let cmsg = config.log.error(context, 'Could not post notification.\n' + error);
                                webhookIO.postToSlack(cmsg);
                            });
                    }
                }
            }
        }
    }

    config.log.info(context, 'Done with expiration queue.');

    return Promise.resolve();
});
*/
