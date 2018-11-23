// Database connection parameters
module.exports.PGHOST = process.env.PGHOST || 'localhost';
module.exports.PGUSER = process.env.PGUSER || 'postgres';
module.exports.PGDATABASE = process.env.PGDATABASE || 'postgres';
module.exports.PGPASSWORD = process.env.PGPASSWORD || 'test';
module.exports.PGPORT = process.env.PGPORT || 5432;
