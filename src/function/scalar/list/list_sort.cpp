#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

// FIXME: use a local state for each thread to increase performance?
// FIXME: benchmark the use of simple_update against using update (if applicable)

struct ListSortBindData : public FunctionData {
	ListSortBindData(/* TODO: parameters */);
	~ListSortBindData() override;

	// TODO: private variables

	unique_ptr<FunctionData> Copy() override;
};

ListSortBindData::ListSortBindData(/* TODO: parameters */) {
}

unique_ptr<FunctionData> ListSortBindData::Copy() {
	return make_unique<ListSortBindData>(/* TODO: parameters */);
}

ListSortBindData::~ListSortBindData() {
}

static void ListSortFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	// TODO
}

static unique_ptr<FunctionData> ListSortBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {
	
	// TODO
	return make_unique<ListSortBindData>(/* TODO: parameters */);
}

ScalarFunction ListSortFun::GetFunction() {
	return ScalarFunction({LogicalType::LIST(LogicalType::ANY)}, LogicalType::LIST(LogicalType::ANY),
	                      ListSortFunction, false, ListSortBind, nullptr, nullptr, nullptr);
}

void ListSortFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_sort", "array_sort"}, GetFunction());
}

} // namespace duckdb