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
    console.log(req.user)

    // Check for any possible filter
    let cluster_status = req.query.status ? req.query.status : '%';
    let cluster_name = req.query.name ? req.query.name : '%';

    // Execute query
    const q = 'select * from cluster';
    query(q, (err, res) => {
        console.log(err ? err.stack : res);
    });
});

router.get('/cluster/:id', auth_verifier, function(req, res) {

});

// Jobs data routes

router.get('/jobs', auth_verifier, function(req, res) {

});

router.get('/job/:id', auth_verifier, function(req, res) {

});

// Authentication routes

router.post('/login', async function (req, res) {
    const username = req.body.username;
    const pwd = req.body.password;

    if (username == null || pwd == null) {
        return res.sendStatus(400).json({
            'reason': 'Bad request',
            'msg': 'no "username" or "password" field specified'
        });
    }

    // Check that username and password match the database
    const q = 'select exists(select 1 from users where email=$1 and ' +
        'password=crypt($2, gen_salt(\'bf\', 8)))';
    const v = [username, pwd];
    try {
        let res = await query(q, v);
        const token = jwt.sign({username: username}, secret);
        return res.send(token);
    } catch (err) {
        return res.sendStatus(401)
    }
});

// Export API router
module.exports = router;
