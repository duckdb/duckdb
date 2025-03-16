#include "duckdb.hpp"
#include <iostream>

int main() {
	try {
		// Open DuckDB database (in-memory for simplicity, change "example.db" for a persistent one)
		duckdb::DuckDB db("../duckdb_benchmark_data/tpch_sf1.duckdb");
		duckdb::Connection conn(db);

		conn.Query("Set threads = 1;");

		// TPC-H Query 16
		std::string query = R"(explain analyze
            SELECT
                p_brand,
                p_type,
                p_size,
                count(DISTINCT ps_suppkey) AS supplier_cnt
            FROM
                partsupp,
                part
            WHERE
                p_partkey = ps_partkey
                AND p_brand <> 'Brand#45'
                AND p_type NOT LIKE 'MEDIUM POLISHED%'
                AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
                AND ps_suppkey NOT IN (
                    SELECT
                        s_suppkey
                    FROM
                        supplier
                    WHERE
                        s_comment LIKE '%Customer%Complaints%')
            GROUP BY
                p_brand,
                p_type,
                p_size
            ORDER BY
                supplier_cnt DESC,
                p_brand,
                p_type,
                p_size;
        )";

		// Execute query
		auto result = conn.Query(query);
		if (result->HasError()) {
			std::cerr << "Query error: " << result->GetError() << "\n";
			return 1;
		} else {
			std::cout << result->ToString() << "\n";
		}
	} catch (std::exception &e) {
		std::cerr << "Exception: " << e.what() << "\n";
		return 1;
	}
	return 0;
}