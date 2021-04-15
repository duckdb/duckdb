#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

struct StructExtractBindData : public FunctionData {
	StructExtractBindData(string key, idx_t index, LogicalType type) : key(move(key)), index(index), type(move(type)) {
	}

	string key;
	idx_t index;
	LogicalType type;

public:
	unique_ptr<FunctionData> Copy() override {
		return make_unique<StructExtractBindData>(key, index, type);
	}
	bool Equals(FunctionData &other_p) override {
		auto &other = (StructExtractBindData &)other_p;
		return key == other.key && index == other.index && type == other.type;
	}
};

static void StructExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (StructExtractBindData &)*func_expr.bind_info;

	// this should be guaranteed by the binder
	auto &vec = args.data[0];

	vec.Verify(args.size());
	if (vec.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vec);
		auto &dict_sel = DictionaryVector::SelVector(vec);
		auto &children = StructVector::GetEntries(child);
		if (info.index >= children.size()) {
			throw Exception("Not enough struct entries for struct_extract");
		}
		auto &struct_child = children[info.index];
		if (struct_child.first != info.key || struct_child.second->GetType() != info.type) {
			throw Exception("Struct key or type mismatch");
		}
		result.Slice(*struct_child.second, dict_sel, args.size());
	} else {
		auto &children = StructVector::GetEntries(vec);
		if (info.index >= children.size()) {
			throw Exception("Not enough struct entries for struct_extract");
		}
		auto &struct_child = children[info.index];
		if (struct_child.first != info.key || struct_child.second->GetType() != info.type) {
			throw Exception("Struct key or type mismatch");
		}
		result.Reference(*struct_child.second);
	}
	result.Verify(args.size());
}

static unique_ptr<FunctionData> StructExtractBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {
	auto &struct_children = arguments[0]->return_type.child_types();
	if (struct_children.empty()) {
		throw Exception("Can't extract something from an empty struct");
	}

	auto &key_child = arguments[1];

	if (key_child->return_type.id() != LogicalTypeId::VARCHAR ||
	    key_child->return_type.id() != LogicalTypeId::VARCHAR || !key_child->IsFoldable()) {
		throw Exception("Key name for struct_extract needs to be a constant string");
	}
	Value key_val = ExpressionExecutor::EvaluateScalar(*key_child.get());
	D_ASSERT(key_val.type().id() == LogicalTypeId::VARCHAR);
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
	bound_function.arguments[0] = arguments[0]->return_type;
	return make_unique<StructExtractBindData>(key, key_index, return_type);
}

ScalarFunction StructExtractFun::GetFunction() {
	return ScalarFunction("struct_extract", {LogicalType::STRUCT, LogicalType::VARCHAR}, LogicalType::ANY,
	                      StructExtractFunction, false, StructExtractBind);
}

void StructExtractFun::RegisterFunction(BuiltinFunctions &set) {
	// the arguments and return types are actually set in the binder function
	auto fun = GetFunction();
	set.AddFunction(fun);
}

} // namespace duckdb
