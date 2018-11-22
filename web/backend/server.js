const express = require('express');
const path = require('path');

// Create Express app
const app = express();

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
    console.log('ping-pong');
    return res.send('pong');
});


// Serve main page while contacting root
app.get('/', function (req, res) {
    res.sendFile(path.join(__dirname, '..', 'build', 'index.html'));
});

// Start listening
app.listen(process.env.PORT || 8080);
