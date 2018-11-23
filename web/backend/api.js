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

// Utils functions

function sendList(res, list) {
    if(list.length <= 0) {
        return res.sendStatus(404)
    }
    return res.json(list)
}

// Cluster data routes

router.get('/clusters', auth_verifier, async function(req, res) {
    const requesting_user = req.user.username;

    // Check for any possible filter
    let cluster_status = req.query.status ? req.query.status : '%';
    let cluster_name = req.query.name ? req.query.name : '%';

    // Execute query
    const q = 'select * from cluster';

    try {
        let qres = await query(q);
        return sendList(qres.rows)
    } catch (err) {
        console.error(err);
        return res.sendStatus(401)
    }
});

router.get('/cluster/:name', auth_verifier, async function(req, res) {
    const requesting_user = req.user.username;

    // Execute query
    const q = 'select * from cluster where name=$1';
    const v = [req.params.name];

    try {
        let qres = await query(q, v);
        return sendList(qres.rows)
    } catch (err) {
        console.error(err);
        return res.sendStatus(401)
    }
});

// Jobs data routes

router.get('/jobs', auth_verifier, async function(req, res) {
    const requesting_user = req.user.username;

    // Check for any possible filter
    let job_status = req.query.status ? req.query.status : '%';
    let job_cluster = req.query.cluster ? req.query.cluster : '%';

    // Execute query
    const q = 'select * from job';

    try {
        let qres = await query(q);
        return sendList(qres.rows)
    } catch (err) {
        console.error(err);
        return res.sendStatus(401)
    }
});

router.get('/job/:id', auth_verifier, async function(req, res) {
    const requesting_user = req.user.username;

    // Execute query
    const q = 'select * from job where id=$1';
    const v = [req.params.name];

    try {
        let qres = await query(q, v);
        return sendList(qres.rows)
    } catch (err) {
        console.error(err);
        return res.sendStatus(401)
    }
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
        'password=crypt($2, password))';
    const v = [username, pwd];

    try {
        let qres = await query(q, v);
        if(qres.rows[0].exists === true) {
            const token = jwt.sign({username: username}, secret);
            res.send(token);
        }
        return res.sendStatus(401);
    } catch (err) {
        console.error(err);
        return res.sendStatus(401)
    }
});

// Export API router
module.exports = router;
