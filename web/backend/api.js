const express = require('express');

const query = require('./db');

// Define API router
const router = express.Router();

// Cluster data routes

router.get('/clusters', function(req, res) {
    // Filter based on cluster status
    let cluster_status = req.query.status;
    if(cluster_status !== null) {
      cluster_status = 'running'
    }

    // Execute query
});

router.get('/cluster/:id', function(req, res) {

});

// Jobs data routes

router.get('/jobs', function(req, res) {

});

router.get('/job/:id', function(req, res) {

});

// Authentication routes

router.post('/login', function(req, res) {

});

// Export API router
module.exports = router;
