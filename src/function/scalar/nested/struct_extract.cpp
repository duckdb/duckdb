#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"

using namespace std;

namespace duckdb {

struct StructExtractBindData : public FunctionData {
	string key;
	idx_t index;
	LogicalType type;

	StructExtractBindData(string key, idx_t index, LogicalType type) : key(key), index(index), type(type) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<StructExtractBindData>(key, index, type);
	}
};

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

static unique_ptr<FunctionData> struct_extract_bind(ClientContext &context, ScalarFunction &bound_function, vector<unique_ptr<Expression>> &arguments) {
	auto &struct_children = arguments[0]->return_type.child_types();
	if (struct_children.size() < 1) {
		throw Exception("Can't extract something from an empty struct");
	}

	auto &key_child = arguments[1];

	if (arguments[1]->return_type.id() != LogicalTypeId::VARCHAR || key_child->return_type.id() != LogicalTypeId::VARCHAR ||
	    !key_child->IsFoldable()) {
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

	bound_function.return_type = return_type;
	arguments.pop_back();
	return make_unique<StructExtractBindData>(key, key_index, return_type);
}

void StructExtractFun::RegisterFunction(BuiltinFunctions &set) {
	// the arguments and return types are actually set in the binder function
	ScalarFunction fun("struct_extract", {LogicalType::STRUCT, LogicalType::VARCHAR}, LogicalType::ANY,
	                   struct_extract_fun, false, struct_extract_bind);
	set.AddFunction(fun);
}

} // namespace duckdb
