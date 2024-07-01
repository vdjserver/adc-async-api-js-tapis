'use strict';

//
// config.js
// Application configuration settings
//
// VDJServer Community Data Portal
// ADC API for VDJServer
// https://vdjserver.org
//
// Copyright (C) 2020 The University of Texas Southwestern Medical Center
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

var path = require('path');
var fs = require('fs');
var yaml = require('js-yaml');

var config = {};

module.exports = config;

function parseBoolean(value)
{
    if (value == 'true') return true;
    else if (value == 1) return true;
    else return false;
}

// General
config.name = 'VDJ-ADC-ASYNC-API';
config.async_port = process.env.API_ASYNC_PORT;
config.lrqdata_path = process.env.LRQDATA_PATH;
config.tapis_version = process.env.TAPIS_VERSION;

// Host user for Corral access
config.hostServiceAccount = process.env.HOST_SERVICE_ACCOUNT;
config.hostServiceGroup = process.env.HOST_SERVICE_GROUP;

// Error/debug reporting
config.debug = parseBoolean(process.env.DEBUG_CONSOLE);

// standard info/error reporting
config.log = {};
config.log.info = function(context, msg, ignore_debug = false) {
    var date = new Date().toLocaleString('en-US', { timeZone: 'America/Chicago' });
    var full_msg = date + ' - ' + config.name + ' INFO (' + context + '): ' + msg;
    if (ignore_debug)
        console.log(full_msg);
    else
        if (config.debug) console.log(full_msg);
    return full_msg;
}

config.log.error = function(context, msg) {
    var date = new Date().toLocaleString('en-US', { timeZone: 'America/Chicago' });
    var full_msg = date + ' - ' + config.name + ' ERROR (' + context + '): ' + msg
    console.error(full_msg);
    return full_msg;
}
config.log.info('config', 'Debug console messages enabled.', true);

// post error messages to a slack channel
config.slackURL = process.env.SLACK_WEBHOOK_URL;

// get service info
var infoFile = path.resolve(__dirname, '../../package.json');
var infoString = fs.readFileSync(infoFile, 'utf8');
var info = JSON.parse(infoString);
config.info = {};
config.info.title = info.name;
config.info.description = info.description;
config.info.version = info.version;
config.info.contact = {
    name: "VDJServer",
    url: "http://vdjserver.org/",
    email: "vdjserver@utsouthwestern.edu"
};
config.info.license = {};
config.info.license.name = info.license;

// get api info
//var apiFile = fs.readFileSync(path.resolve(__dirname, '../api/swagger/adc-api.yaml'), 'utf8');
//var apiSpec = yaml.safeLoad(apiFile);
//config.info.api = apiSpec['info'];

// get schema info
//var schemaFile = fs.readFileSync(path.resolve(__dirname, './airr-schema.yaml'), 'utf8');
//var schemaSpec = yaml.safeLoad(schemaFile);
//config.info.schema = schemaSpec['Info'];

// constraints
config.max_size = 1000;
config.info.max_size = 1000;

// TODO: limited at the moment
config.large_query_size = 2 * 1024;
config.large_lrq_query_size = 50 * 1024;
//config.max_query_size = 6 * 1024;
//config.info.max_query_size = 6 * 1024;
config.max_query_size = 2 * 1024 * 1024;
config.info.max_query_size = 2 * 1024 * 1024;

// async API settings
config.async = {};
config.async.enable_poll = parseBoolean(process.env.API_ASYNC_ENABLE_POLL);
config.async.enable_expire = parseBoolean(process.env.API_ASYNC_ENABLE_EXPIRE);
config.async.lifetime = 5 * 24 * 60 * 60; // 5 days in secs
//config.async.lifetime = 60 * 60; // 1 hr for testing
config.async.max_uses = 1000; // postit attempts
config.async.max_size = 500 * 1024 * 1024; // 500M
//config.async.max_size = 30 * 1024 * 1024; // 30M for testing

