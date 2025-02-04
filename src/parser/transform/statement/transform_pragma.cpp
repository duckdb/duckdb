#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/parser/statement/set_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> Transformer::TransformPragma(duckdb_libpgquery::PGPragmaStmt &stmt) {
	auto result = make_uniq<PragmaStatement>();
	auto &info = *result->info;

	info.name = stmt.name;
	// parse the arguments, if any
	if (stmt.args) {
		for (auto cell = stmt.args->head; cell != nullptr; cell = cell->next) {
			auto node = PGPointerCast<duckdb_libpgquery::PGNode>(cell->data.ptr_value);
			auto expr = TransformExpression(node);

			if (expr->GetExpressionType() == ExpressionType::COMPARE_EQUAL) {
				auto &comp = expr->Cast<ComparisonExpression>();
				if (comp.left->GetExpressionType() != ExpressionType::COLUMN_REF) {
					throw ParserException("Named parameter requires a column reference on the LHS");
				}
				auto &columnref = comp.left->Cast<ColumnRefExpression>();
				info.named_parameters.insert(make_pair(columnref.GetName(), std::move(comp.right)));
			} else if (expr->GetExpressionType() == ExpressionType::COLUMN_REF) {
				auto &colref = expr->Cast<ColumnRefExpression>();
				if (!colref.IsQualified()) {
					info.parameters.emplace_back(make_uniq<ConstantExpression>(Value(colref.GetColumnName())));
				} else {
					info.parameters.emplace_back(make_uniq<ConstantExpression>(Value(expr->ToString())));
				}
			} else {
				info.parameters.emplace_back(std::move(expr));
			}
		}
	}
	// now parse the pragma type
	switch (stmt.kind) {
	case duckdb_libpgquery::PG_PRAGMA_TYPE_NOTHING: {
		if (!info.parameters.empty() || !info.named_parameters.empty()) {
			throw InternalException("PRAGMA statement that is not a call or assignment cannot contain parameters");
		}
		break;
	case duckdb_libpgquery::PG_PRAGMA_TYPE_ASSIGNMENT:
		if (info.parameters.size() != 1) {
			throw ParserException("PRAGMA statement with assignment should contain exactly one parameter");
		}
		if (!info.named_parameters.empty()) {
			throw InternalException("PRAGMA statement with assignment cannot have named parameters");
		}
		// SQLite does not distinguish between:
		// "PRAGMA table_info='integers'"
		// "PRAGMA table_info('integers')"
		// for compatibility, any pragmas that match the SQLite ones are parsed as calls
		case_insensitive_set_t sqlite_compat_pragmas {"table_info"};
		if (sqlite_compat_pragmas.find(info.name) != sqlite_compat_pragmas.end()) {
			break;
		}
		auto set_statement =
		    make_uniq<SetVariableStatement>(info.name, std::move(info.parameters[0]), SetScope::AUTOMATIC);
		return std::move(set_statement);
	}
	case duckdb_libpgquery::PG_PRAGMA_TYPE_CALL:
		break;
	default:
		throw InternalException("Unknown pragma type");
	}

	return std::move(result);
}

} // namespace duckdb
