//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/statement_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Statement Types
//===--------------------------------------------------------------------===//
enum class StatementType : uint8_t {
	INVALID,      // invalid statement type
	SELECT,       // select statement type
	INSERT,       // insert statement type
	UPDATE,       // update statement type
	CREATE,       // create statement type
	DELETE,       // delete statement type
	PREPARE,      // prepare statement type
	EXECUTE,      // execute statement type
	ALTER,        // alter statement type
	TRANSACTION,  // transaction statement type,
	COPY,         // copy type
	ANALYZE,      // analyze type
	VARIABLE_SET, // variable set statement type
	CREATE_FUNC,  // create func statement type
	EXPLAIN,      // explain statement type
	DROP,         // DROP statement type
	PRAGMA,       // PRAGMA statement type
	VACUUM,       // VACUUM statement type
};

string StatementTypeToString(StatementType type);

} // namespace duckdb
