#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"

using namespace std;

namespace duckdb {

static void list_value_fun(DataChunk &args, ExpressionState &state, Vector &result) {
	//	auto &func_expr = (BoundFunctionExpression &)state.expr;
	//	auto &info = (VariableReturnBindData &)*func_expr.bind_info;

	assert(result.type == TypeId::LIST);
	auto list_child = make_unique<ChunkCollection>();
	ListVector::SetEntry(result, move(list_child));

	auto &cc = ListVector::GetEntry(result);
	DataChunk append_vals;
	vector<TypeId> types;
	if (args.column_count() > 0) {
		types.push_back(args.GetTypes()[0]);
		append_vals.Initialize(types);
		append_vals.SetCardinality(1);
	}
	result.vector_type = VectorType::CONSTANT_VECTOR;
	for (idx_t i = 0; i < args.column_count(); i++) {
		if (args.data[i].vector_type != VectorType::CONSTANT_VECTOR) {
			result.vector_type = VectorType::FLAT_VECTOR;
		}
	}

	auto result_data = FlatVector::GetData<list_entry_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i].offset = cc.count;
		for (idx_t col_idx = 0; col_idx < args.column_count(); col_idx++) {
			append_vals.SetValue(0, 0, args.GetValue(col_idx, i).CastAs(types[0])); // FIXME evil pattern
			cc.Append(append_vals);
		}
		result_data[i].length = args.column_count();
	}
	result.Verify(args.size());
}

static unique_ptr<FunctionData> list_value_bind(BoundFunctionExpression &expr, ClientContext &context) {
	SQLType stype(SQLTypeId::LIST);

	// collect names and deconflict, construct return type
	assert(expr.arguments.size() == expr.children.size());

	if (expr.children.size() > 0) {
		stype.child_type.push_back(make_pair("", expr.arguments[0]));
	}

	// this is more for completeness reasons
	expr.sql_return_type = stype;
	return make_unique<VariableReturnBindData>(stype);
}

void ListValueFun::RegisterFunction(BuiltinFunctions &set) {
	// the arguments and return types are actually set in the binder function
	ScalarFunction fun("list_value", {}, SQLType::LIST, list_value_fun, false, list_value_bind);
	fun.varargs = SQLType::ANY;
	set.AddFunction(fun);
}

} // namespace duckdb
