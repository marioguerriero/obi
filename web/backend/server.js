// Copyright 2018 Delivery Hero Germany
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
//     Unless required by applicable law or agreed to in writing, software
//     distributed under the License is distributed on an "AS IS" BASIS,
//     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//     See the License for the specific language governing permissions and
//     limitations under the License.

const express = require('express');
const path = require('path');
const bodyParser = require('body-parser');

// Create Express app
const app = express();

// Support JSON and URL encoded bodies
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({
    extended: true
}));

// Logging capabilities
const morgan = require('morgan');
const logger = process.env.NODE_ENV === 'production' ?
    morgan('common') : morgan('dev');
app.use(logger);

// Static resources serving
app.use(express.static(path.join(__dirname, '..', 'build')));

// API routing
const api = require('./api');
app.use('/api', api);

// Debug endpoint
app.get('/ping', function (req, res) {
    return res.send('pong');
});

// Serve main page while contacting root
app.get('/', function (req, res) {
    res.sendFile(path.join(__dirname, '..', 'build', 'index.html'));
});

// Start listening
const port = process.env.PORT || 8080;
app.listen(port);
console.log('Start listening on port', port);
