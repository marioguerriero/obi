// Copyright 2018 Delivery Hero Germany
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

const fs = require('fs');
const path = require('path');

const credentials_base = process.env.CREDENTIALS_FS || '/etc/credentials';

// Database connection parameters
module.exports.PGHOST = process.env.STOLON_PROXY_DNS_NAME || 'localhost';
module.exports.PGPORT = process.env.STOLON_PROXY_PORT || 5432;
module.exports.PGDATABASE = process.env.PGDATABASE || 'postgres';
try {
    module.exports.PGUSER = fs.readFileSync(path.join(credentials_base, 'username'), encoding='utf-8');
} catch (err) {
    module.exports.PGUSER = 'postgres';
}
try {
    module.exports.PGPASSWORD = fs.readFileSync(path.join(credentials_base, 'password'), encoding='utf-8') || 'test';
} catch (err) {
    module.exports.PGPASSWORD = 'test';
}
