//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/statement_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Statement Types
//===--------------------------------------------------------------------===//
enum class StatementType : uint8_t {
	INVALID_STATEMENT,      // invalid statement type
	SELECT_STATEMENT,       // select statement type
	INSERT_STATEMENT,       // insert statement type
	UPDATE_STATEMENT,       // update statement type
	CREATE_STATEMENT,       // create statement type
	DELETE_STATEMENT,       // delete statement type
	PREPARE_STATEMENT,      // prepare statement type
	EXECUTE_STATEMENT,      // execute statement type
	ALTER_STATEMENT,        // alter statement type
	TRANSACTION_STATEMENT,  // transaction statement type,
	COPY_STATEMENT,         // copy type
	ANALYZE_STATEMENT,      // analyze type
	VARIABLE_SET_STATEMENT, // variable set statement type
	CREATE_FUNC_STATEMENT,  // create func statement type
	EXPLAIN_STATEMENT,      // explain statement type
	DROP_STATEMENT,         // DROP statement type
	EXPORT_STATEMENT,       // EXPORT statement type
	PRAGMA_STATEMENT,       // PRAGMA statement type
	VACUUM_STATEMENT,       // VACUUM statement type
	CALL_STATEMENT,         // CALL statement type
	SET_STATEMENT,          // SET statement type
	LOAD_STATEMENT,         // LOAD statement type
	RELATION_STATEMENT,
	EXTENSION_STATEMENT,
	LOGICAL_PLAN_STATEMENT,
	ATTACH_STATEMENT,
	DETACH_STATEMENT,
	MULTI_STATEMENT,
	COPY_DATABASE_STATEMENT,
	UPDATE_EXTENSIONS_STATEMENT,
};

DUCKDB_API string StatementTypeToString(StatementType type);

enum class StatementReturnType : uint8_t {
	QUERY_RESULT, // the statement returns a query result (e.g. for display to the user)
	CHANGED_ROWS, // the statement returns a single row containing the number of changed rows (e.g. an insert stmt)
	NOTHING       // the statement returns nothing
};

string StatementReturnTypeToString(StatementReturnType type);

//! A struct containing various properties of a SQL statement
struct StatementProperties {
	StatementProperties()
	    : requires_valid_transaction(true), allow_stream_result(false), bound_all_parameters(true),
	      return_type(StatementReturnType::QUERY_RESULT), parameter_count(0), always_require_rebind(false) {
	}

	//! The set of databases this statement will read from
	unordered_set<string> read_databases;
	//! The set of databases this statement will modify
	unordered_set<string> modified_databases;
	//! Whether or not the statement requires a valid transaction. Almost all statements require this, with the
	//! exception of ROLLBACK
	bool requires_valid_transaction;
	//! Whether or not the result can be streamed to the client
	bool allow_stream_result;
	//! Whether or not all parameters have successfully had their types determined
	bool bound_all_parameters;
	//! What type of data the statement returns
	StatementReturnType return_type;
	//! The number of prepared statement parameters
	idx_t parameter_count;
	//! Whether or not the statement ALWAYS requires a rebind
	bool always_require_rebind;

	bool IsReadOnly() {
		return modified_databases.empty();
	}
};

} // namespace duckdb
