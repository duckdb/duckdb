#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformMultiAssignRef(duckdb_libpgquery::PGMultiAssignRef &root) {
	// Multi assignment for the ROW function
	if (root.source->type == duckdb_libpgquery::T_PGFuncCall) {
		auto func = PGCast<duckdb_libpgquery::PGFuncCall>(*root.source);

		// Too many columns (ie. (x, y) = (1, 2, 3) )
		if (root.ncolumns < func.args->length) {
			throw ParserException("Too many columns for Multi Assignment");
		}

		// Get the expression corresponding with the current column
		idx_t idx = 1;
		auto list = func.args->head;
		while (list && idx < root.colno) {
			list = list->next;
			++idx;
		}

		// Not enough columns (ie. (x, y, z) = (1, 2) )
		if (!list) {
			throw ParserException("Not enough columns for Multi Assignment");
		}
		return TransformExpression(reinterpret_cast<duckdb_libpgquery::PGNode *>(list->data.ptr_value));
	}

	// Multi assignent for a single expression
	return TransformExpression(root.source);
}

} // namespace duckdb
