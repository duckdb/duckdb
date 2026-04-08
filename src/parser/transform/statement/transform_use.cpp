#include <string>
#include <utility>

#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/statement/set_statement.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/set_scope.hpp"
#include "duckdb/common/exception/parser_exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "nodes/parsenodes.hpp"

namespace duckdb {

unique_ptr<SetStatement> Transformer::TransformUse(duckdb_libpgquery::PGUseStmt &stmt) {
	auto qualified_name = TransformQualifiedName(*stmt.name);
	if (!IsInvalidCatalog(qualified_name.catalog)) {
		throw ParserException("Expected \"USE database\" or \"USE database.schema\"");
	}
	string name;
	if (IsInvalidSchema(qualified_name.schema)) {
		name = KeywordHelper::WriteOptionallyQuoted(qualified_name.name, '"');
	} else {
		name = KeywordHelper::WriteOptionallyQuoted(qualified_name.schema, '"') + "." +
		       KeywordHelper::WriteOptionallyQuoted(qualified_name.name, '"');
	}
	auto name_expr = make_uniq<ConstantExpression>(Value(name));
	return make_uniq<SetVariableStatement>("schema", std::move(name_expr), SetScope::AUTOMATIC);
}

} // namespace duckdb
