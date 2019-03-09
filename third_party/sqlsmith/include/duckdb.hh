/// @file
/// @brief schema and dut classes for DuckDB

#ifndef DUCKDB_HH
#define DUCKDB_HH

#include "duckdb.hpp"

#include "dut.hh"
#include "relmodel.hh"
#include "schema.hh"

struct duckdb_connection {
	std::unique_ptr<duckdb::DuckDB> database;
	std::unique_ptr<duckdb::Connection> connection;
	char *zErrMsg = 0;
	int rc;
	void q(const char *query);
	duckdb_connection(std::string &conninfo);
};

struct schema_duckdb : schema, duckdb_connection {
	schema_duckdb(std::string &conninfo, bool no_catalog);
	virtual std::string quote_name(const std::string &id) {
		return id;
	}
};

struct dut_duckdb : dut_base, duckdb_connection {
	virtual void test(const std::string &stmt);
	dut_duckdb(std::string &conninfo);
};
#endif
