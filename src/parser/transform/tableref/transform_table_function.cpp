#include "duckdb/common/exception.hpp"
#include "duckdb/parser/srf_utils.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<TableRef> Transformer::TransformRangeFunction(duckdb_libpgquery::PGRangeFunction &root) {
	if (root.is_rowsfrom) {
		// ROWS FROM(f1(), f2()) → unnest(list1, list2, ...)
		auto unnest_func = make_uniq<FunctionExpression>("unnest", vector<unique_ptr<ParsedExpression>> {});
		vector<string> func_names;
		idx_t func_idx = 0;
		for (auto cell = root.functions->head; cell; cell = cell->next, func_idx++) {
			auto function_sublist = PGPointerCast<duckdb_libpgquery::PGList>(cell->data.ptr_value);
			D_ASSERT(function_sublist->length == 2);
			auto call_tree = PGPointerCast<duckdb_libpgquery::PGNode>(function_sublist->head->data.ptr_value);

			unique_ptr<ParsedExpression> func_expr;
			switch (call_tree->type) {
			case duckdb_libpgquery::T_PGFuncCall: {
				auto func_call = PGPointerCast<duckdb_libpgquery::PGFuncCall>(call_tree.get());
				func_expr = TransformFuncCall(*func_call);
				break;
			}
			case duckdb_libpgquery::T_PGSQLValueFunction:
				func_expr =
				    TransformSQLValueFunction(*PGPointerCast<duckdb_libpgquery::PGSQLValueFunction>(call_tree.get()));
				break;
			default:
				throw ParserException("ROWS FROM: not a function call");
			}

			if (func_expr->GetExpressionClass() == ExpressionClass::FUNCTION) {
				func_names.push_back(func_expr->Cast<FunctionExpression>().function_name);
			} else {
				func_names.push_back("column" + to_string(func_idx + 1));
			}
			unnest_func->children.push_back(WrapTableFuncAsList(std::move(func_expr), func_idx));
		}

		auto result = make_uniq<TableFunctionRef>();
		if (root.ordinality) {
			result->with_ordinality = OrdinalityType::WITH_ORDINALITY;
		}
		result->function = std::move(unnest_func);
		result->alias = TransformAlias(root.alias, result->column_name_alias);
		if (result->column_name_alias.empty()) {
			result->column_name_alias = std::move(func_names);
		}
		if (root.sample) {
			result->sample = TransformSampleOptions(root.sample);
		}
		return std::move(result);
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
