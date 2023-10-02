#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformMultiAssignRef(duckdb_libpgquery::PGMultiAssignRef &root) {
	// Multi assignment for the ROW function
	if (root.source->type == duckdb_libpgquery::T_PGFuncCall) {
		auto func = PGCast<duckdb_libpgquery::PGFuncCall>(*root.source);

		// Explicitly only allow ROW function
		char const *function_name =
		    PGPointerCast<duckdb_libpgquery::PGValue>(func.funcname->tail->data.ptr_value)->val.str;
		if (function_name == nullptr || strlen(function_name) != 3 || strncmp(function_name, "row", 3) != 0) {
			return TransformExpression(root.source);
		}

		// Too many columns (ie. (x, y) = (1, 2, 3) )
		if (root.ncolumns < func.args->length) {
			throw ParserException(
			    "Could not perform multiple assignment, target only expects %d values, %d were provided", root.ncolumns,
			    func.args->length);
		}

		// Get the expression corresponding with the current column
		idx_t idx = 1;
		auto list = func.args->head;
		while (list && idx < static_cast<idx_t>(root.colno)) {
			list = list->next;
			++idx;
		}

		// Not enough columns (ie. (x, y, z) = (1, 2) )
		if (!list) {
			throw ParserException(
			    "Could not perform multiple assignment, target expects %d values, only %d were provided", root.ncolumns,
			    func.args->length);
		}
		return TransformExpression(reinterpret_cast<duckdb_libpgquery::PGNode *>(list->data.ptr_value));
	}
	return TransformExpression(root.source);
}

} // namespace duckdb
