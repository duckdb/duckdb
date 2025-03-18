#include "duckdb.hpp"

#include <iostream>
#include <chrono> // For timing

int main() {
	// Open DuckDB database (in-memory for simplicity, change "example.db" for a persistent one)
	std::string path = "../duckdb_benchmark_data/imdb.duckdb";
	duckdb::DuckDB db(path);
	duckdb::Connection conn(db);

	conn.Query("SET threads = 1;");

	std::string query = R"(
		explain analyze
		           SELECT MIN(mc.note) AS production_note,
		       MIN(t.title) AS movie_title,
		       MIN(t.production_year) AS movie_year
		FROM company_type AS ct,
		     info_type AS it,
		     movie_companies AS mc,
		     movie_info_idx AS mi_idx,
		     title AS t
		WHERE ct.kind = 'production companies'
		  AND it.info = 'top 250 rank'
		  AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
		  AND (mc.note LIKE '%(co-production)%'
		       OR mc.note LIKE '%(presents)%')
		  AND ct.id = mc.company_type_id
		  AND t.id = mc.movie_id
		  AND t.id = mi_idx.movie_id
		  AND mc.movie_id = mi_idx.movie_id
		  AND it.id = mi_idx.info_type_id;)";

	// Execute query twice to warm up
	conn.Query(query);
	conn.Query(query);

	// Start timer
	auto start = std::chrono::high_resolution_clock::now();

	// Execute last query
	auto result = conn.Query(query);

	// Stop timer
	auto end = std::chrono::high_resolution_clock::now();

	// Calculate elapsed time in milliseconds
	double elapsed_ms = std::chrono::duration<double, std::milli>(end - start).count();

	// Check result and print execution time
	if (result->HasError()) {
		std::cerr << "Query error: " << result->GetError() << "\n";
		return 1;
	} else {
		std::cout << result->ToString() << "\n";
		std::cout << "Execution Time: " << elapsed_ms << " ms\n";
	}

	return 0;
}
