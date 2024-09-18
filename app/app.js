'use strict';

//
// app.js
// Application entry point
//
// VDJServer Community Data Portal
// ADC API Asynchronous Extension for VDJServer
// https://vdjserver.org
//
// Copyright (C) 2021-2024 The University of Texas Southwestern Medical Center
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

var express = require('express');
var bodyParser   = require('body-parser');
var openapi = require('express-openapi');
var path = require('path');
var fs = require('fs');
var yaml = require('js-yaml');
var $RefParser = require("@apidevtools/json-schema-ref-parser");
var airr = require('airr-js');
var vdj_schema = require('vdjserver-schema');

// Express app
var app = module.exports = express();
var context = 'app';

// Server environment config
var config = require('./config/config');

// CORS
var allowCrossDomain = function(request, response, next) {
    response.header('Access-Control-Allow-Origin', '*');
    response.header('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS');
    response.header('Access-Control-Allow-Headers', 'Content-Type, Authorization, Content-Length, X-Requested-With');

    // intercept OPTIONS method
    if ('OPTIONS' === request.method) {
        response.status(200).end();
    }
    else {
        next();
    }
};

// Server Settings
app.set('port', config.async_port);
app.use(allowCrossDomain);
// trust proxy so we can get client IP
app.set('trust proxy', true);
app.redisConfig = {
    port: 6379,
    host: 'vdjr-redis'
};

// Tapis
var tapisSettings = require('vdj-tapis-js/tapisSettings');
var tapisIO = tapisSettings.get_default_tapis(config);
var ServiceAccount = tapisIO.serviceAccount;
var GuestAccount = tapisIO.guestAccount;
var authController = tapisIO.authController;
var webhookIO = require('vdj-tapis-js/webhookIO');

// Mongo
var mongoSettings = require('vdj-tapis-js/mongoSettings');
mongoSettings.set_config(config);

// Controllers
var statusController = require('./api/controllers/status');
var asyncController = require('./api/controllers/async-query');

// Downgrade to host vdj user
// This is also so that the /vdjZ Corral file volume can be accessed,
// as it is restricted to the TACC vdj account.
// read/write access is required.
config.log.info(context, 'Downgrading to host user: ' + config.hostServiceAccount, true);
process.setgid(config.hostServiceGroup);
process.setuid(config.hostServiceAccount);
config.log.info(context, 'Current uid: ' + process.getuid(), true);
config.log.info(context, 'Current gid: ' + process.getgid(), true);

// Verify we can login with guest account
ServiceAccount.getToken()
    .then(function(serviceToken) {
        config.log.info(context, 'Successfully acquired service token.', true);
        return GuestAccount.getToken();
    })
    .then(function(guestToken) {
        config.log.info(context, 'Successfully acquired guest token.', true);

        // wait for the AIRR schema to be loaded
        return airr.load_schema();
    })
    .then(function(schema) {
        config.log.info(context, 'Loaded AIRR Schema version ' + airr.get_info()['version'], true);

        // wait for the VDJServer schema to be loaded
        return vdj_schema.load_schema();
    })
    .then(function(schema) {
        config.log.info(context, 'Loaded VDJServer Schema version ' + vdj_schema.get_info()['version'], true);

        // Connect schema to vdj-tapis
        tapisIO.init_with_schema(vdj_schema);

        // Load ADC Async API
        var apiFile = path.resolve(__dirname, 'api/swagger/adc-api-async.yaml');
        config.log.info(context, 'Using ADC API Async specification: ' + apiFile, true);
        var api_spec = yaml.safeLoad(fs.readFileSync(apiFile, 'utf8'));
        config.log.info(context, 'Loaded ADC API Async version: ' + api_spec.info.version, true);

        // Load internal admin API
        var notifyFile = path.resolve(__dirname, 'api/swagger/async-admin.yaml');
        config.log.info(context, 'async admin API specification: ' + notifyFile, true);
        var notify_spec = yaml.safeLoad(fs.readFileSync(notifyFile, 'utf8'));
        // copy paths
        for (var p in notify_spec['paths']) {
            api_spec['paths'][p] = notify_spec['paths'][p];
        }

        // dereference the API spec
        return $RefParser.dereference(api_spec);
    })
    .then(function(api_schema) {
        //console.log(api_schema);

        // wrap the operations functions to catch syntax errors and such
        // we do not get a good stack trace with the middleware error handler
        var try_function = async function (request, response, the_function) {
            try {
                await the_function(request, response);
            } catch (e) {
                console.error(e);
                console.error(e.stack);
                throw e;
            }
        };

        openapi.initialize({
            apiDoc: api_schema,
            app: app,
            promiseMode: true,
            errorMiddleware: function(err, req, res, next) {
                console.log('Got an error!');
                console.log(JSON.stringify(err));
                console.trace("Here I am!");
                if (err.status) res.status(err.status).json(err.errors);
                else res.status(500).json(err.errors);
            },
            consumesMiddleware: {
                'application/json': bodyParser.json({limit: config.max_query_size})
                //'application/x-www-form-urlencoded': bodyParser.urlencoded({extended: true})
            },
            securityHandlers: {
                admin_authorization: authController.adminAuthorization
            },
            operations: {
                // service status and info
                get_service_status: async function(req, res) { return try_function(req, res, statusController.getStatus); },
                get_info: async function(req, res) { return try_function(req, res, statusController.getInfo); },

                // queries
                get_query_status: async function(req, res) { return try_function(req, res, asyncController.getQueryStatus); },
                async_repertoire: async function(req, res) { return try_function(req, res, asyncController.asyncQueryRepertoire); },
                async_rearrangement: async function(req, res) { return try_function(req, res, asyncController.asyncQueryRearrangement); },
                async_clone: async function(req, res) { return try_function(req, res, asyncController.asyncQueryClone); },
                async_notify: async function(req, res) { return try_function(req, res, asyncController.asyncNotify); },
            }
        });

        // Start listening on port
        return new Promise(function(resolve, reject) {
            app.listen(app.get('port'), function() {
                config.log.info(context, 'VDJServer ADC ASYNC API service listening on port ' + app.get('port'), true);
                resolve();
            });
        });
    })
    .then(function() {
        config.log.info(context, 'VDJServer ADC ASYNC API, triggering queue', true);
        AsyncQueue.triggerQueue();

/*        if (config.async.enable_poll) {
            config.log.info(context, 'Polling ENABLED for LRQ');
            AsyncQueue.triggerPolling();
        } else config.log.info(context, 'Polling DISABLED for LRQ');
        if (config.async.enable_expire) {
            config.log.info(context, 'Expiration ENABLED for async queries');
            AsyncQueue.triggerExpiration();
        } else config.log.info(context, 'Expiration DISABLED for async queries'); */
    })
    .catch(function(error) {
        var msg = config.log.error(context, 'Service could not be start.\n' + error);
        //console.trace(msg);
        webhookIO.postToSlack(msg);
        // continue in case its a temporary error
        //process.exit(1);
    });

// Queue Management
var AsyncQueue = require('./api/queues/async-queue');
//AsyncQueue.processQueryJobs();
