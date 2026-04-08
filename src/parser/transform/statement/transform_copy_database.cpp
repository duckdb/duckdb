#include <string>
#include <utility>

#include "duckdb/parser/statement/copy_database_statement.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parsed_data/pragma_info.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "nodes/parsenodes.hpp"

namespace duckdb {

unique_ptr<SQLStatement> Transformer::TransformCopyDatabase(duckdb_libpgquery::PGCopyDatabaseStmt &stmt) {
	if (stmt.copy_database_flag) {
		// copy a specific subset of the database
		CopyDatabaseType type;
		if (StringUtil::Equals(stmt.copy_database_flag, "schema")) {
			type = CopyDatabaseType::COPY_SCHEMA;
		} else if (StringUtil::Equals(stmt.copy_database_flag, "data")) {
			type = CopyDatabaseType::COPY_DATA;
		} else {
			throw NotImplementedException("Unsupported flag for COPY DATABASE");
		}
		return make_uniq<CopyDatabaseStatement>(stmt.from_database, stmt.to_database, type);
	} else {
		auto result = make_uniq<PragmaStatement>();
		result->info->name = "copy_database";
		result->info->parameters.emplace_back(make_uniq<ConstantExpression>(Value(stmt.from_database)));
		result->info->parameters.emplace_back(make_uniq<ConstantExpression>(Value(stmt.to_database)));
		return std::move(result);
	}
}

} // namespace duckdb
