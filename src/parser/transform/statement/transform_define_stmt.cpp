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

	// Build a PRAGMA statement: PRAGMA create_text_search_dictionary('name', ...)
	auto result = make_uniq<PragmaStatement>();
	result->info->name = "create_text_search_dictionary";

	// First parameter: dictionary name
	result->info->parameters.push_back(make_uniq<ConstantExpression>(Value(dict_name)));

	// Second parameter: IF NOT EXISTS flag
	result->info->parameters.push_back(make_uniq<ConstantExpression>(Value::BOOLEAN(stmt.if_not_exists)));

	// Remaining parameters: each option as "key=value" strings
	if (stmt.definition) {
		for (auto cell = stmt.definition->head; cell != nullptr; cell = cell->next) {
			auto def = PGPointerCast<duckdb_libpgquery::PGDefElem>(cell->data.ptr_value);
			string key = def->defname;
			string value;
			if (def->arg) {
				switch (def->arg->type) {
				case duckdb_libpgquery::T_PGString:
					value = PGPointerCast<duckdb_libpgquery::PGValue>(def->arg)->val.str;
					break;
				case duckdb_libpgquery::T_PGInteger:
					value = to_string(PGPointerCast<duckdb_libpgquery::PGValue>(def->arg)->val.ival);
					break;
				case duckdb_libpgquery::T_PGFloat:
					value = PGPointerCast<duckdb_libpgquery::PGValue>(def->arg)->val.str;
					break;
				default:
					value = "true";
					break;
				}
			} else {
				value = "true";
			}
			result->info->parameters.push_back(make_uniq<ConstantExpression>(Value(key + "=" + value)));
		}
	}

	return std::move(result);
}

} // namespace duckdb
