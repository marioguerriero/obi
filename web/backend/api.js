const express = require('express');

const secret = require('./secret');
const jwt = require('jsonwebtoken');
const jwt_middleware = require('express-jwt');

const auth_verifier = jwt_middleware({
    secret: secret
});

const query = require('./db');

// Define API router
const router = express.Router();

// Cluster data routes

router.get('/clusters', auth_verifier, function(req, res) {
    // Check for any possible filter
    let cluster_status = req.query.status ? req.query.status : '%';
    let cluster_name = req.query.name ? req.query.name : '%';

    // Execute query
});

router.get('/cluster/:id', auth_verifier, function(req, res) {

});

// Jobs data routes

router.get('/jobs', auth_verifier, function(req, res) {

});

router.get('/job/:id', auth_verifier, function(req, res) {

});

// Authentication routes

router.post('/login', function(req, res) {
    // Check that username and password match the database

    // Generate and send JWT token
    const token = jwt.sign({} /* payload */, secret);
    res.send(token);
});

// Export API router
module.exports = router;
