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
if (config.tapis_version == 2) config.log.info(context, 'Using Tapis V2 API', true);
else if (config.tapis_version == 3) config.log.info(context, 'Using Tapis V3 API', true);
else {
    config.log.error(context, 'Invalid Tapis version, check TAPIS_VERSION environment variable');
    process.exit(1);
}
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
        config.log.info(context, 'Loaded AIRR Schema version ' + airr.get_info()['version']);

        // wait for the VDJServer schema to be loaded
        return vdj_schema.load_schema();
    })
    .then(function(schema) {
        config.log.info(context, 'Loaded VDJServer Schema version ' + vdj_schema.get_info()['version']);

        // Connect schema to vdj-tapis
        if (tapisIO == tapisV3) tapisV3.init_with_schema(vdj_schema);

        // Load ADC Async API
        var apiFile = path.resolve(__dirname, 'api/swagger/adc-api-async.yaml');
        config.log.info(context, 'Using ADC API Async specification: ' + apiFile);
        var api_spec = yaml.safeLoad(fs.readFileSync(apiFile, 'utf8'));
        config.log.info(context, 'Loaded ADC API Async version: ' + api_spec.info.version);

        // Load internal admin API
        var notifyFile = path.resolve(__dirname, 'api/swagger/async-admin.yaml');
        config.log.info(context, 'notify API specification: ' + notifyFile);
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
                user_authorization: authController.userAuthorization,
                admin_authorization: authController.adminAuthorization,
                project_authorization: authController.projectAuthorization
            },
            operations: {
                // service status and info
                get_service_status: statusController.getStatus,
                get_info: statusController.getInfo,

                // queries
                get_query_status: asyncController.getQueryStatus,
                async_repertoire: asyncController.asyncQueryRepertoire,
                async_rearrangement: asyncController.asyncQueryRearrangement,
                async_clone: asyncController.asyncQueryClone,
                async_notify: asyncController.asyncNotify
            }
        });

        // Start listening on port
        return new Promise(function(resolve, reject) {
            app.listen(app.get('port'), function() {
                config.log.info(context, 'VDJServer ADC ASYNC API service listening on port ' + app.get('port'));
                resolve();
            });
        });
    })
    .then(function() {
        if (config.async.enable_poll) {
            config.log.info(context, 'Polling ENABLED for LRQ');
            AsyncQueue.triggerPolling();
        } else config.log.info(context, 'Polling DISABLED for LRQ');
        if (config.async.enable_expire) {
            config.log.info(context, 'Expiration ENABLED for async queries');
            AsyncQueue.triggerExpiration();
        } else config.log.info(context, 'Expiration DISABLED for async queries');
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
