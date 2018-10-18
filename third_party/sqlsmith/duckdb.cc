#include "duckdb.hh"
#include "dbgen.hpp"

#include <cassert>
#include <cstring>
#include <iostream>
#include <stdexcept>

#include <regex>

using namespace duckdb;
using namespace std;

static regex e_syntax("Query Error: syntax error at or near .*");

duckdb_connection::duckdb_connection(string &conninfo) {
	// in-memory database
	database = make_unique<DuckDB>(nullptr);
	connection = make_unique<DuckDBConnection>(*database);
}

void duckdb_connection::q(const char *query) {
	auto result = connection->Query(query);
	if (!result->GetSuccess()) {
		throw runtime_error(result->GetErrorMessage());
	}
}

schema_duckdb::schema_duckdb(std::string &conninfo, bool no_catalog)
    : duckdb_connection(conninfo) {
	// generate empty TPC-H schema
	tpch::dbgen(0, *database);

	cerr << "Loading tables...";
	auto result = connection->Query(
	    "SELECT * FROM sqlite_master() WHERE type IN ('table', 'view')");
	if (!result->GetSuccess()) {
		throw runtime_error(result->GetErrorMessage());
	}
	for (size_t i = 0; i < result->size(); i++) {
		auto type = result->collection.GetValue(0, i).str_value;
		auto name = result->collection.GetValue(2, i).str_value;
		bool view = type == "view";
		table tab(name, "main", !view, !view);
		tables.push_back(tab);
	}
	cerr << "done." << endl;

	if (tables.size() == 0) {
		throw std::runtime_error("No tables available in catalog!");
	}

	cerr << "Loading columns and constraints...";

	for (auto t = tables.begin(); t != tables.end(); ++t) {
		result = connection->Query("PRAGMA table_info('" + t->name + "')");
		if (!result->GetSuccess()) {
			throw runtime_error(result->GetErrorMessage());
		}
		for (size_t i = 0; i < result->size(); i++) {
			auto name = result->collection.GetValue(1, i).str_value;
			auto type = result->collection.GetValue(2, i).str_value;
			column c(name, sqltype::get(type));
			t->columns().push_back(c);
		}
	}

	cerr << "done." << endl;

#define BINOP(n, t)                                                            \
	do {                                                                       \
		op o(#n, sqltype::get(#t), sqltype::get(#t), sqltype::get(#t));        \
		register_operator(o);                                                  \
	} while (0)

	// BINOP(||, TEXT);
	BINOP(*, INTEGER);
	BINOP(/, INTEGER);

	BINOP(+, INTEGER);
	BINOP(-, INTEGER);

	// BINOP(>>, INTEGER);
	// BINOP(<<, INTEGER);

	// BINOP(&, INTEGER);
	// BINOP(|, INTEGER);

	BINOP(<, INTEGER);
	BINOP(<=, INTEGER);
	BINOP(>, INTEGER);
	BINOP(>=, INTEGER);

	BINOP(=, INTEGER);
	BINOP(<>, INTEGER);
	BINOP(IS, INTEGER);
	BINOP(IS NOT, INTEGER);

	BINOP(AND, INTEGER);
	BINOP(OR, INTEGER);

#define FUNC(n, r)                                                             \
	do {                                                                       \
		routine proc("", "", sqltype::get(#r), #n);                            \
		register_routine(proc);                                                \
	} while (0)

#define FUNC1(n, r, a)                                                         \
	do {                                                                       \
		routine proc("", "", sqltype::get(#r), #n);                            \
		proc.argtypes.push_back(sqltype::get(#a));                             \
		register_routine(proc);                                                \
	} while (0)

#define FUNC2(n, r, a, b)                                                      \
	do {                                                                       \
		routine proc("", "", sqltype::get(#r), #n);                            \
		proc.argtypes.push_back(sqltype::get(#a));                             \
		proc.argtypes.push_back(sqltype::get(#b));                             \
		register_routine(proc);                                                \
	} while (0)

#define FUNC3(n, r, a, b, c)                                                   \
	do {                                                                       \
		routine proc("", "", sqltype::get(#r), #n);                            \
		proc.argtypes.push_back(sqltype::get(#a));                             \
		proc.argtypes.push_back(sqltype::get(#b));                             \
		proc.argtypes.push_back(sqltype::get(#c));                             \
		register_routine(proc);                                                \
	} while (0)

	// FUNC(last_insert_rowid, INTEGER);
	// FUNC(random, INTEGER);
	// FUNC(sqlite_source_id, TEXT);
	// FUNC(sqlite_version, TEXT);
	// FUNC(total_changes, INTEGER);

	FUNC1(abs, INTEGER, REAL);
	// FUNC1(hex, TEXT, TEXT);
	// FUNC1(length, INTEGER, TEXT);
	// FUNC1(lower, TEXT, TEXT);
	// FUNC1(ltrim, TEXT, TEXT);
	// FUNC1(quote, TEXT, TEXT);
	// FUNC1(randomblob, TEXT, INTEGER);
	// FUNC1(round, INTEGER, REAL);
	// FUNC1(rtrim, TEXT, TEXT);
	// FUNC1(soundex, TEXT, TEXT);
	// FUNC1(sqlite_compileoption_get, TEXT, INTEGER);
	// FUNC1(sqlite_compileoption_used, INTEGER, TEXT);
	// FUNC1(trim, TEXT, TEXT);
	// FUNC1(typeof, TEXT, INTEGER);
	// FUNC1(typeof, TEXT, NUMERIC);
	// FUNC1(typeof, TEXT, REAL);
	// FUNC1(typeof, TEXT, TEXT);
	// FUNC1(unicode, INTEGER, TEXT);
	// FUNC1(upper, TEXT, TEXT);
	// FUNC1(zeroblob, TEXT, INTEGER);

	// FUNC2(glob, INTEGER, TEXT, TEXT);
	// FUNC2(instr, INTEGER, TEXT, TEXT);
	// FUNC2(like, INTEGER, TEXT, TEXT);
	// FUNC2(ltrim, TEXT, TEXT, TEXT);
	// FUNC2(rtrim, TEXT, TEXT, TEXT);
	// FUNC2(trim, TEXT, TEXT, TEXT);
	// FUNC2(round, INTEGER, REAL, INTEGER);
	// FUNC2(substr, TEXT, TEXT, INTEGER);

	// FUNC3(substr, TEXT, TEXT, INTEGER, INTEGER);
	// FUNC3(replace, TEXT, TEXT, TEXT, TEXT);

#define AGG(n, r, a)                                                           \
	do {                                                                       \
		routine proc("", "", sqltype::get(#r), #n);                            \
		proc.argtypes.push_back(sqltype::get(#a));                             \
		register_aggregate(proc);                                              \
	} while (0)

	AGG(avg, INTEGER, INTEGER);
	AGG(avg, REAL, REAL);
	AGG(count, INTEGER, REAL);
	AGG(count, INTEGER, TEXT);
	AGG(count, INTEGER, INTEGER);
	// AGG(group_concat, TEXT, TEXT);
	AGG(max, REAL, REAL);
	AGG(max, INTEGER, INTEGER);
	AGG(min, REAL, REAL);
	AGG(min, INTEGER, INTEGER);
	AGG(sum, REAL, REAL);
	AGG(sum, INTEGER, INTEGER);
	// AGG(total, REAL, INTEGER);
	// AGG(total, REAL, REAL);

	booltype = sqltype::get("INTEGER");
	inttype = sqltype::get("INTEGER");

	internaltype = sqltype::get("internal");
	arraytype = sqltype::get("ARRAY");

	true_literal = "1";
	false_literal = "0";

	generate_indexes();
}

dut_duckdb::dut_duckdb(std::string &conninfo) : duckdb_connection(conninfo) {
	cerr << "Generating TPC-H...";
	tpch::dbgen(0.1, *database);
	cerr << "done." << endl;
	// q("PRAGMA main.auto_vacuum = 2");
}

void dut_duckdb::test(const std::string &stmt) {
	auto result = connection->Query(stmt);
	if (!result->GetSuccess()) {
		auto error = result->GetErrorMessage().c_str();
		try {
			if (regex_match(error, e_syntax))
				throw dut::syntax(error);
			else
				throw dut::failure(error);
		} catch (dut::failure &e) {
			throw;
		}
	}
}
