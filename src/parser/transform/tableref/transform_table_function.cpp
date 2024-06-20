#include "duckdb/common/exception.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<TableRef> Transformer::TransformRangeFunction(duckdb_libpgquery::PGRangeFunction &root) {
	if (root.ordinality) {
		throw NotImplementedException("WITH ORDINALITY not implemented");
	}
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
	switch (call_tree->type) {
	case duckdb_libpgquery::T_PGFuncCall: {
		auto func_call = PGPointerCast<duckdb_libpgquery::PGFuncCall>(call_tree.get());
		result->function = TransformFuncCall(*func_call);
		if (root.coldeflist) {
			duckdb_libpgquery::PGListCell *cell;
			for_each_cell(cell, root.coldeflist->head) {
				auto def_elem = PGPointerCast<duckdb_libpgquery::PGColumnDef>(cell->data.ptr_value);
				LogicalType target_type =
				    (!def_elem->typeName) ? LogicalType::ANY : TransformTypeName(*def_elem->typeName);
				string colname;
				if (def_elem->colname) {
					colname = def_elem->colname;
				}
				result->column_name_alias.push_back(colname);
				result->column_type_hint.push_back(target_type);
			}
		}

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
