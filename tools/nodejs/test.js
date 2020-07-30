const duckdb = require('.');
con = new duckdb.connect();
res = con.query("SELECT 42::integer as a, 42::string as b UNION ALL SELECT 1, 2");
console.log(res.data);
con.disconnect();
