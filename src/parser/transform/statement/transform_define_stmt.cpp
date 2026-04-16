#include "duckdb/common/exception.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

unique_ptr<SQLStatement> Transformer::TransformDefineStmt(duckdb_libpgquery::PGDefineStmt &stmt) {
	if (stmt.kind != duckdb_libpgquery::PG_OBJECT_TSDICTIONARY) {
		throw NotImplementedException("DefineStmt kind %d not supported", static_cast<int>(stmt.kind));
	}

	// Extract dictionary name from defnames list
	string dict_name;
	if (stmt.defnames) {
		for (auto cell = stmt.defnames->head; cell != nullptr; cell = cell->next) {
			auto val = PGPointerCast<duckdb_libpgquery::PGValue>(cell->data.ptr_value);
			if (!dict_name.empty()) {
				dict_name += ".";
			}
			dict_name += val->val.str;
		}
	}

	// Build a PRAGMA statement: PRAGMA create_text_search_dictionary('name', if_not_exists, key := value, ...)
	auto result = make_uniq<PragmaStatement>();
	result->info->name = "create_text_search_dictionary";

	// First parameter: dictionary name
	result->info->parameters.push_back(make_uniq<ConstantExpression>(Value(dict_name)));

	// Second parameter: IF NOT EXISTS flag
	result->info->parameters.push_back(make_uniq<ConstantExpression>(Value::BOOLEAN(stmt.if_not_exists)));

	// Named parameters: tokenizer options
	if (stmt.definition) {
		for (auto cell = stmt.definition->head; cell != nullptr; cell = cell->next) {
			auto def = PGPointerCast<duckdb_libpgquery::PGDefElem>(cell->data.ptr_value);

			unique_ptr<ParsedExpression> param_expr = def->arg
			    ? TransformValue(*PGPointerCast<duckdb_libpgquery::PGValue>(def->arg))
			    : make_uniq<ConstantExpression>(Value::BOOLEAN(true));

			auto [_, inserted] = result->info->named_parameters.emplace(def->defname, std::move(param_expr));
			if (!inserted) {
				throw InvalidInputException("conflicting or redundant options: \"%s\" specified more than once", def->defname);
			}
		}
	}

	return std::move(result);
}

} // namespace duckdb
