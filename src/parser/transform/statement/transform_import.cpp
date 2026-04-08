#include <string>

#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parsed_data/pragma_info.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "nodes/parsenodes.hpp"

namespace duckdb {

unique_ptr<PragmaStatement> Transformer::TransformImport(duckdb_libpgquery::PGImportStmt &stmt) {
	auto result = make_uniq<PragmaStatement>();
	result->info->name = "import_database";
	result->info->parameters.emplace_back(make_uniq<ConstantExpression>(Value(stmt.filename)));
	return result;
}

} // namespace duckdb
