'use strict';

//
// status.js
// Status and info end points
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

var StatusController = {};
module.exports = StatusController;

// Server environment config
var config = require('../../config/config');

// Tapis
var tapisSettings = require('vdj-tapis-js/tapisSettings');
var tapisIO = tapisSettings.get_default_tapis();
var ServiceAccount = tapisIO.serviceAccount;
var GuestAccount = tapisIO.guestAccount;
var authController = tapisIO.authController;
var webhookIO = require('vdj-tapis-js/webhookIO');
var adc_mongo_query = require('vdj-tapis-js/adc_mongo_query');

// service status
StatusController.getStatus = function(req, res) {
    var context = 'StatusController.getStatus';

    // Verify we can login with guest account
    GuestAccount.getToken()
        .then(function(guestToken) {
            res.json({"result":"success"});
        })
        .catch(function(error) {
            var msg = config.log.error(context, 'Could not acquire guest token.\n.' + error);
            res.status(500).json({"message":"Internal service error."});
            webhookIO.postToSlack(msg);
        });
}

// service info
StatusController.getInfo = function(req, res) {
    // Respond with service info
    res.json(config.info);
}

// not implemented stub
StatusController.notImplemented = function(req, res) {
    res.status(500).json({"message":"Not implemented."});
}
