#include "duckdb/common/exception.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<TableRef> Transformer::TransformRangeFunction(postgres::RangeFunction *root) {
	if (root->lateral) {
		throw NotImplementedException("LATERAL not implemented");
	}
	if (root->ordinality) {
		throw NotImplementedException("WITH ORDINALITY not implemented");
	}
	if (root->is_rowsfrom) {
		throw NotImplementedException("ROWS FROM() not implemented");
	}
	if (root->functions->length != 1) {
		throw NotImplementedException("Need exactly one function");
	}
	auto function_sublist = (postgres::List *)root->functions->head->data.ptr_value;
	assert(function_sublist->length == 2);
	auto call_tree = (postgres::Node *)function_sublist->head->data.ptr_value;
	auto coldef = function_sublist->head->next->data.ptr_value;

	assert(call_tree->type == postgres::T_FuncCall);
	if (coldef) {
		throw NotImplementedException("Explicit column definition not supported yet");
	}
	// transform the function call
	auto result = make_unique<TableFunctionRef>();
	result->function = TransformFuncCall((postgres::FuncCall *)call_tree);
	result->alias = TransformAlias(root->alias);
	return move(result);
}
