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

const { Client } = require('pg');

const config = require('./config');

// Define connection parameters
const client = new Client({
    user: config.PGUSER,
    host: config.PGHOST,
    database: config.PGDATABASE,
    password: config.PGPASSWORD,
    port: config.PGPORT
});

// Connect to server's database
async function connect() {
    await client.connect()
        .catch(e => console.error(e.stack));
}

// Generic database query function
function query(q, values=null) {
    // Return a query promise
    return client.query(q, values)
        .then(res => res)
        .catch(e => console.error(e.stack))
}

module.exports.connect = connect;
module.exports.query = query;
