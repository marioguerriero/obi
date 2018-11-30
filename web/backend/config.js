const fs = require('fs');
const path = require('path');

const credentials_base = process.env.CREDENTIALS_FS || '/etc/credentials';

// Database connection parameters
module.exports.PGHOST = process.env.STOLON_PROXY_DNS_NAME || 'localhost';
module.exports.PGPORT = process.env.STOLON_PROXY_PORT || 5432;
module.exports.PGDATABASE = process.env.PGDATABASE || 'postgres';
try {
    module.exports.PGUSER = fs.readFileSync(path.join(credentials_base, 'username'));
} catch (err) {
    module.exports.PGUSER = 'postgres';
}
try {
    module.exports.PGPASSWORD = fs.readFileSync(path.join(credentials_base, 'password')) || 'test';
} catch (err) {
    module.exports.PGPASSWORD = 'test';
}
