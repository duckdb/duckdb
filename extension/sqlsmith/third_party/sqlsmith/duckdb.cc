#include "duckdb.hh"

#include <cassert>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <chrono>

#include <regex>

using namespace duckdb;
using namespace std;

static regex e_syntax(".*syntax error at or near .*");
static regex e_internal(".*INTERNAL.*");

sqlsmith_duckdb_connection::sqlsmith_duckdb_connection(duckdb::DatabaseInstance &database) {
	// in-memory database
	connection = make_unique<Connection>(database);
}

void sqlsmith_duckdb_connection::q(const char *query) {
	auto result = connection->Query(query);
	if (!result->success) {
		throw runtime_error(result->error);
	}
}

schema_duckdb::schema_duckdb(duckdb::DatabaseInstance &database, bool no_catalog, bool verbose_output)
    : sqlsmith_duckdb_connection(database) {
	// generate empty TPC-H schema
	if (verbose_output)
		cerr << "Loading tables...";
	auto result = connection->Query("SELECT * FROM sqlite_master WHERE type IN ('table', 'view')");
	if (!result->success) {
		throw runtime_error(result->error);
	}
	for (size_t i = 0; i < result->RowCount(); i++) {
		auto type = StringValue::Get(result->GetValue(0, i));
		auto name = StringValue::Get(result->GetValue(2, i));
		bool view = type == "view";
		table tab(name, "main", !view, !view);
		tables.push_back(tab);
	}
	if (verbose_output)
		cerr << "done." << endl;

	if (tables.size() == 0) {
		throw std::runtime_error("No tables available in catalog!");
	}
	if (verbose_output)
		cerr << "Loading columns and constraints...";

	for (auto t = tables.begin(); t != tables.end(); ++t) {
		result = connection->Query("PRAGMA table_info('" + t->name + "')");
		if (!result->success) {
			throw runtime_error(result->error);
		}
		for (size_t i = 0; i < result->RowCount(); i++) {
			auto name = StringValue::Get(result->GetValue(1, i));
			auto type = StringValue::Get(result->GetValue(2, i));
			column c(name, sqltype::get(type));
			t->columns().push_back(c);
		}
	}

	if (verbose_output)
		cerr << "done." << endl;

	Connection con(database);
	auto query_result = con.Query(R"(
SELECT function_name, parameter_types, return_type, function_type
FROM duckdb_functions()
WHERE NOT(has_side_effects)
 AND (function_type='aggregate' or function_type='scalar');
    )");

	for (auto &row : *query_result) {
		auto function_name = row.GetValue<string>(0);
		auto parameter_types = row.GetValue<Value>(1);
		auto return_type = row.GetValue<string>(2);
		auto function_type = row.GetValue<string>(3);
		auto &params = ListValue::GetChildren(parameter_types);
		if (function_type == "scalar") {
			// check if this is an operator or a function
			bool is_operator = false;
			for (auto ch : function_name) {
				if (!StringUtil::CharacterIsAlpha(ch) && !StringUtil::CharacterIsDigit(ch) && ch != '_') {
					is_operator = true;
					break;
				}
			}
			if (is_operator) {
				// operator
				if (params.size() == 2) {
					auto lparam = StringValue::Get(params[0]);
					auto rparam = StringValue::Get(params[1]);
					op o(function_name, sqltype::get(lparam), sqltype::get(rparam), sqltype::get(return_type));
					register_operator(o);
				}
			} else {
				// function
				routine proc("", "", sqltype::get(return_type), function_name);
				for (auto &param : params) {
					auto param_name = StringValue::Get(params[0]);
					proc.argtypes.push_back(sqltype::get(param_name));
				}
				register_routine(proc);
			}
		} else if (function_type == "aggregate") {
			routine proc("", "", sqltype::get(return_type), function_name);
			for (auto &param : params) {
				auto param_name = StringValue::Get(params[0]);
				proc.argtypes.push_back(sqltype::get(param_name));
			}
			register_aggregate(proc);
		} else {
			throw std::runtime_error("unrecognized function type in sqlsmith");
		}
	}

	booltype = sqltype::get("BOOLEAN");
	inttype = sqltype::get("INTEGER");

	internaltype = sqltype::get("internal");
	arraytype = sqltype::get("ARRAY");

	true_literal = "1";
	false_literal = "0";

	auto &type_list = sqltype::get_types();
	for (auto &kv : type_list) {
		types.push_back(kv.second);
	}

	generate_indexes(verbose_output);
}

dut_duckdb::dut_duckdb(duckdb::DatabaseInstance &database) : sqlsmith_duckdb_connection(database) {
}

volatile bool is_active = false;
// timeout is 10ms * TIMEOUT_TICKS
#define TIMEOUT_TICKS 50

void sleep_thread(Connection *connection) {
	for (size_t i = 0; i < TIMEOUT_TICKS && is_active; i++) {
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	if (is_active) {
		connection->Interrupt();
	}
}

void dut_duckdb::test(const std::string &stmt) {
	is_active = true;
	thread interrupt_thread(sleep_thread, connection.get());
	auto result = connection->Query(stmt);
	is_active = false;
	interrupt_thread.join();

	if (!result->success) {
		auto error = result->error.c_str();
		if (regex_match(error, e_internal)) {
			throw dut::broken(error);
		}
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
