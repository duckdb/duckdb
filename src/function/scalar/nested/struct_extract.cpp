#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"

using namespace std;

namespace duckdb {

static void struct_extract_fun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (StructExtractBindData &)*func_expr.bind_info;

	// this should be guaranteed by the binder
	assert(args.column_count() == 1);
	auto &vec = args.data[0];

	vec.Verify(args.size());
	if (vec.vector_type == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vec);
		auto &dict_sel = DictionaryVector::SelVector(vec);
		auto &children = StructVector::GetEntries(child);
		if (info.index >= children.size()) {
			throw Exception("Not enough struct entries for struct_extract");
		}
		auto &struct_child = children[info.index];
		if (struct_child.first != info.key || struct_child.second->type != info.type) {
			throw Exception("Struct key or type mismatch");
		}
		result.Slice(*struct_child.second, dict_sel, args.size());
	} else {
		auto &children = StructVector::GetEntries(vec);
		if (info.index >= children.size()) {
			throw Exception("Not enough struct entries for struct_extract");
		}
		auto &struct_child = children[info.index];
		if (struct_child.first != info.key || struct_child.second->type != info.type) {
			throw Exception("Struct key or type mismatch");
		}
		result.Reference(*struct_child.second);
	}
	result.Verify(args.size());
}

static unique_ptr<FunctionData> struct_extract_bind(BoundFunctionExpression &expr, ClientContext &context) {
	// the binder should fix this for us.
	assert(expr.children.size() == 2);
	assert(expr.arguments.size() == expr.children.size());
	assert(expr.arguments[0].id() == LogicalTypeId::STRUCT);
	assert(expr.children[0]->return_type.id() == LogicalTypeId::STRUCT);

	auto &struct_children = expr.arguments[0].child_types();
	if (struct_children.size() < 1) {
		throw Exception("Can't extract something from an empty struct");
	}

	auto &key_child = expr.children[1];

	if (expr.arguments[1].id() != LogicalTypeId::VARCHAR || key_child->return_type.id() != LogicalTypeId::VARCHAR ||
	    !key_child->IsScalar()) {
		throw Exception("Key name for struct_extract needs to be a constant string");
	}
	Value key_val = ExpressionExecutor::EvaluateScalar(*key_child.get());
	assert(key_val.type().id() == LogicalTypeId::VARCHAR);
	if (key_val.is_null || key_val.str_value.length() < 1) {
		throw Exception("Key name for struct_extract needs to be neither NULL nor empty");
	}
	string key = StringUtil::Lower(key_val.str_value);

	LogicalType return_type;
	idx_t key_index = 0;
	bool found_key = false;

	for (size_t i = 0; i < struct_children.size(); i++) {
		auto &child = struct_children[i];
		if (child.first == key) {
			found_key = true;
			key_index = i;
			return_type = child.second;
			break;
		}
	}
	if (!found_key) {
		throw Exception("Could not find key in struct");
	}

	expr.return_type = return_type;
	expr.children.pop_back();
	return make_unique<StructExtractBindData>(key, key_index, return_type.InternalType());
}

void StructExtractFun::RegisterFunction(BuiltinFunctions &set) {
	// the arguments and return types are actually set in the binder function
	ScalarFunction fun("struct_extract", {LogicalType::STRUCT, LogicalType::VARCHAR}, LogicalType::ANY, struct_extract_fun, false,
	                   struct_extract_bind);
	set.AddFunction(fun);
}

} // namespace duckdb
