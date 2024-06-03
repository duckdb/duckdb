#include <iostream>
#include "duckdb.hpp"

using namespace std;
using namespace std::chrono;
using namespace duckdb;

void print_query(duckdb::Connection &con, const string &qry_str) {
    auto result = con.Query(qry_str);
    if (result->HasError()) {
        cout << result->GetError();
    } else {
        cout << result->ToString() << endl;
    }
}

void time_query(duckdb::Connection &con, const string &qry_str) {
    // Get starting timepoint
    auto start = high_resolution_clock::now();

    // Call the function
    auto result = con.Query(qry_str);

    // Get ending timepoint
    auto stop = high_resolution_clock::now();

    // Get duration. Substart timepoints to
    // get duration. To cast it to proper unit
    // use duration cast method
    auto duration = duration_cast<microseconds>(stop - start);

    cout << "Time taken by query: "
         << duration.count() << " microseconds" << endl;
}

void explain_query(duckdb::Connection &con, const string &qry_str) {
    con.Query("SET explain_output = 'all';");
    auto result = con.Query(string("EXPLAIN ") + qry_str);
    if (result->HasError()) {
        cout << result->GetError();
    } else {
        cout << result->ToString() << endl;
    }
}

/*

Example demonstrating slow performance from https://github.com/duckdb/duckdb/issues/10214
Projection pushdown on DISTINCT ON doesn't work

*/

void test1(void) {
    DuckDB db(nullptr);
    Connection con(db);

    // create a table
    int ncol = 10;
    int nrow = 10;
    stringstream tbl;
    tbl << "CREATE TABLE push_d (";
    for(int i=0; i<ncol; i++) {
        if(i > 0) {
            tbl << ", ";
        }
        tbl << "col" << i << " INTEGER";
    }
    tbl << ")";
    // cout << tbl.str() << endl;
    con.Query(tbl.str());

    // insert rows into the table
    stringstream rowstr;
    rowstr << "INSERT INTO push_d VALUES ";
    for(int row=0; row<nrow; row++) {
        rowstr << "(";
        for(int col=0; col<ncol; col++) {
            if(col >0) {
                rowstr << ",";
            }
            rowstr << row*ncol + col;
        }
        rowstr << "), ";
    }
    // cout << rowstr.str() << endl;
    con.Query(rowstr.str());

    // cout << "Example Table:" << endl;
    // print_query(con, "SELECT * FROM push_d");


    string query_template =
            "SELECT col0\n"
            "FROM \n"
            "(\n"
            "    SELECT \n"
            "    DISTINCT ON (floor(col0))\n"
            "    {columns}\n"
            "    FROM push_d\n"
            "    ORDER by col0 DESC\n"
            ")";


    string param = "{columns}";
    string qry_col0 = string(query_template).replace(query_template.find(param),param.length(),"col0");
    string qry_col_star = string(query_template).replace(query_template.find(param),param.length(),"*");

    // cout << "Query:\n" << qry_col_star << endl;

    print_query(con, qry_col_star);


    cout << "Explain col0:" << endl;
    explain_query(con, qry_col0);
    cout << "\n";
    cout << "Explain *:" << endl;
    explain_query(con, qry_col_star);


    cout << "Time col0:" << endl;
    time_query(con, qry_col0);
    cout << "\n";
    cout << "Time using *:" << endl;
    time_query(con, qry_col_star);
}

/* test_10087.test
 *
 * statement ok
CREATE TABLE t0(c1 INT);

statement ok
INSERT INTO t0(c1) VALUES (1);

statement ok
CREATE VIEW v0(c0, c1, c2) AS SELECT '1', true, t0.c1 FROM t0 ORDER BY -1-2 LIMIT 2;

statement ok
SELECT v0.c2 FROM v0 WHERE (NOT (v0.c1 IS NOT NULL));
 */

// Based on test_10087.test
void test2(void) {
    DuckDB db(nullptr);
    Connection con(db);

    string qry1 = "CREATE TABLE t0(c1 INT);";
    con.Query(qry1);

    string qry2 = "INSERT INTO t0(c1) VALUES (1);";
    con.Query(qry2);

    string qry3 = "CREATE VIEW v0(c0, c1, c2) AS SELECT '1', true, t0.c1 FROM t0 ORDER BY -1-2 LIMIT 2;";
    con.Query(qry3);

    string qry4 = "SELECT v0.c2 FROM v0 WHERE (NOT (v0.c1 IS NOT NULL));";
    explain_query(con, qry4);
    print_query(con, qry4);
}


int main(void) {
    test1();
    // test2();
    return 0;
}


