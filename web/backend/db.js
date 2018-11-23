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
client.connect();

// Generic database query function
function query(q, values=null) {
    // Return a query promise
    return client.query(q, values)
        .then(res => res)
        .catch(e => console.error(e.stack))
}

module.exports = query;
