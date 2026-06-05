#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformMultiAssignRef(duckdb_libpgquery::PGMultiAssignRef &root) {
	// Early-out, if the root is not a function call.
	if (root.source->type != duckdb_libpgquery::T_PGFuncCall) {
		return TransformExpression(root.source);
	}

	auto func = PGCast<duckdb_libpgquery::PGFuncCall>(*root.source);

	// Only allow ROW function.
	auto function = PGPointerCast<duckdb_libpgquery::PGValue>(func.funcname->tail->data.ptr_value);
	char const *function_name = function->val.str;
	if (!function_name || !StringUtil::CIEquals(function_name, "row")) {
		return TransformExpression(root.source);
	}

	// Too many columns, e.g., (x, y) != (1, 2, 3).
	int64_t value_count = func.args ? func.args->length : 0;
	if (int64_t(root.ncolumns) < value_count || !func.args) {
		throw ParserException("Could not perform assignment, expected %d values, got %d", root.ncolumns, value_count);
	}

	// Get the expression corresponding with the current column.
	int64_t idx = 1;
	auto list = func.args->head;
	while (list && idx < int64_t(root.colno)) {
		list = list->next;
		++idx;
	}

	// Not enough columns, e.g., (x, y, z) != (1, 2).
	if (!list) {
		throw ParserException("Could not perform assignment, expected %d values, got %d", root.ncolumns,
		                      func.args->length);
	}

	auto node = PGPointerCast<duckdb_libpgquery::PGNode>(list->data.ptr_value);
	return TransformExpression(node);
}

} // namespace duckdb
