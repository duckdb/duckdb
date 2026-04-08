#include <string>
#include <utility>

#include "duckdb/common/exception.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/enums/ordinality_request_type.hpp"
#include "duckdb/common/exception/parser_exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/parser/parsed_data/sample_options.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/tableref.hpp"
#include "nodes/nodes.hpp"
#include "nodes/parsenodes.hpp"
#include "nodes/pg_list.hpp"
#include "nodes/primnodes.hpp"

namespace duckdb {

unique_ptr<TableRef> Transformer::TransformRangeFunction(duckdb_libpgquery::PGRangeFunction &root) {
	if (root.is_rowsfrom) {
		throw NotImplementedException("ROWS FROM() not implemented");
	}
	if (root.functions->length != 1) {
		throw NotImplementedException("Need exactly one function");
	}
	auto function_sublist = PGPointerCast<duckdb_libpgquery::PGList>(root.functions->head->data.ptr_value);
	D_ASSERT(function_sublist->length == 2);
	auto call_tree = PGPointerCast<duckdb_libpgquery::PGNode>(function_sublist->head->data.ptr_value);
	auto coldef = function_sublist->head->next->data.ptr_value;

	if (coldef) {
		throw NotImplementedException("Explicit column definition not supported yet");
	}
	// transform the function call
	auto result = make_uniq<TableFunctionRef>();
	if (root.ordinality) {
		result->with_ordinality = OrdinalityType::WITH_ORDINALITY;
	}
	switch (call_tree->type) {
	case duckdb_libpgquery::T_PGFuncCall: {
		auto func_call = PGPointerCast<duckdb_libpgquery::PGFuncCall>(call_tree.get());
		result->function = TransformFuncCall(*func_call);
		SetQueryLocation(*result, func_call->location);
		break;
	}
	case duckdb_libpgquery::T_PGSQLValueFunction:
		result->function =
		    TransformSQLValueFunction(*PGPointerCast<duckdb_libpgquery::PGSQLValueFunction>(call_tree.get()));
		break;
	default:
		throw ParserException("Not a function call or value function");
	}
	result->alias = TransformAlias(root.alias, result->column_name_alias);
	if (root.sample) {
		result->sample = TransformSampleOptions(root.sample);
	}
	return std::move(result);
}

} // namespace duckdb
