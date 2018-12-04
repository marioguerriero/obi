// Copyright 2018 
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
const { check, validationResult } = require('express-validator/check');
const { sanitize } = require('express-validator/filter');

const secret = require('./secret');
const jwt = require('jsonwebtoken');
const jwt_middleware = require('express-jwt');

const auth_verifier = jwt_middleware({
    secret: secret
});

const jwt_options = {
    expiresIn: '7 days'
};

const DB = require('./db');

// Instantiate database connection
DB.connect();

// Define API router
const router = express.Router();

// Utils functions

function sendList(res, list, forceList=false) {
    if(list.length <= 0) {
        return res.sendStatus(404)
    }
    if(list.length === 1 && !forceList)
        return res.json(list[0]);
    return res.json(list)
}

// Cluster data routes

router.get('/clusters', auth_verifier, [ sanitize(['status', 'name']) ], async function(req, res) {
    const requesting_user = req.user.username;

    // Check for any possible filter
    let cluster_status = req.query.status ? req.query.status + '%' : '%';
    let cluster_name = req.query.name ? req.query.name + '%' : '%';

    // Execute query
    const q = 'select * from cluster where status like $1 and name like $2 order by creationtimestamp desc';
    const v = [cluster_status, cluster_name];

    try {
        let qres = await DB.query(q, v);
        return sendList(res, qres.rows, true);
    } catch (err) {
        console.error(err);
        return res.sendStatus(401)
    }
});

router.get('/cluster/:name', auth_verifier, [ sanitize(['name']) ], async function(req, res) {
    const requesting_user = req.user.username;

    // Execute query
    const q = 'select * from cluster where name=$1';
    const v = [req.params.name];

    try {
        let qres = await DB.query(q, v);
        return sendList(res, qres.rows);
    } catch (err) {
        console.error(err);
        return res.sendStatus(401)
    }
});

// Jobs data routes

router.get('/jobs', auth_verifier, [ sanitize(['status', 'cluster']) ], async function(req, res) {
    const requesting_user = req.user.username;

    // Check for any possible filter
    let job_status = req.query.status ? req.query.status + '%' : '%';
    let job_cluster = req.query.cluster ? req.query.cluster + '%' : '%';

    // Execute query
    const q = 'select * from job where status like $1 and clustername like $2';
    const v = [job_status, job_cluster];

    try {
        let qres = await DB.query(q, v);
        return sendList(res, qres.rows);
    } catch (err) {
        console.error(err);
        return res.sendStatus(401)
    }
});

router.get('/job/:id', auth_verifier, [ sanitize(['id']) ], async function(req, res) {
    const requesting_user = req.user.username;

    // Execute query
    const q = 'select * from job where id=$1';
    const v = [req.params.id];

    try {
        let qres = await DB.query(q, v);
        return sendList(res, qres.rows);
    } catch (err) {
        console.error(err);
        return res.sendStatus(401)
    }
});

// User data routes

router.get('/user/:id', auth_verifier, [ sanitize(['id']) ], async function(req, res) {
    const requesting_user = req.user.username;

    // Execute query
    const q = 'select email from users where id=$1';
    const v = [req.params.id];

    try {
        let qres = await DB.query(q, v);
        return sendList(res, qres.rows);
    } catch (err) {
        console.error(err);
        return res.sendStatus(401)
    }
});

// Authentication routes

router.post('/login', [
        check('username').isEmail()
    ], async function (req, res) {
    // Finds the validation errors in this request and wraps them in an object with handy functions
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
        return res.status(422).json({ errors: errors.array() });
    }

    const username = req.body.username;
    const pwd = req.body.password;

    if (username == null || pwd == null) {
        return res.sendStatus(400)
    }

    // Check that username and password match the database
    const q = 'select exists(select 1 from users where email=$1 and ' +
        'password=crypt($2, password))';
    const v = [username, pwd];

    try {
        let qres = await DB.query(q, v);

        if(qres.rows[0].exists === true) {
            const token = jwt.sign({username: username}, secret, jwt_options);
            return res.send(token);
        }
        return res.sendStatus(401);
    } catch (err) {
        console.error(err);
        return res.sendStatus(401)
    }
});

// Export API router
module.exports = router;
