/// @file
/// @brief schema and dut classes for DuckDB

#ifndef DUCKDB_HH
#define DUCKDB_HH

#include "duckdb.hpp"

#include "dut.hh"
#include "relmodel.hh"
#include "schema.hh"

struct sqlsmith_duckdb_connection {
	duckdb::unique_ptr<duckdb::DuckDB> database;
	duckdb::unique_ptr<duckdb::Connection> connection;
	char *zErrMsg = 0;
	int rc;
	void q(const char *query);
	sqlsmith_duckdb_connection(duckdb::DatabaseInstance &database);
};

struct schema_duckdb : schema, sqlsmith_duckdb_connection {
	schema_duckdb(duckdb::DatabaseInstance &database, bool no_catalog, bool verbose_output);
	virtual std::string quote_name(const std::string &id) {
		return id;
	}
};

struct dut_duckdb : dut_base, sqlsmith_duckdb_connection {
	virtual void test(const std::string &stmt);
	dut_duckdb(duckdb::DatabaseInstance &database);
};
#endif
