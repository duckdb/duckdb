#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

struct ListTransformBindData : public FunctionData {
	ListTransformBindData(const LogicalType &stype_p, unique_ptr<Expression> lambda_function);
	~ListTransformBindData() override;

	LogicalType stype;
	unique_ptr<Expression> lambda_function;

public:
	bool Equals(const FunctionData &other_p) const override;
	unique_ptr<FunctionData> Copy() const override;
};

ListTransformBindData::ListTransformBindData(const LogicalType &stype_p, unique_ptr<Expression> lambda_function_p)
    : stype(stype_p), lambda_function(move(lambda_function_p)) {
}

unique_ptr<FunctionData> ListTransformBindData::Copy() const {
	return make_unique<ListTransformBindData>(stype, lambda_function->Copy());
}

bool ListTransformBindData::Equals(const FunctionData &other_p) const {
	auto &other = (ListTransformBindData &)other_p;
	return lambda_function->Equals(other.lambda_function.get());
}

ListTransformBindData::~ListTransformBindData() {
}

static void ListTransformFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	D_ASSERT(args.ColumnCount() == 1);
	auto count = args.size();
	Vector &lists = args.data[0];

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &result_validity = FlatVector::Validity(result);

	if (lists.GetType().id() == LogicalTypeId::SQLNULL) {
		result_validity.SetInvalid(0);
		return;
	}

	// get the lambda function
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (ListTransformBindData &)*func_expr.bind_info;

	// get the child vector
	auto lists_size = ListVector::GetListSize(lists);
	auto &child_vector = ListVector::GetEntry(lists);
	VectorData child_data;
	child_vector.Orrify(lists_size, child_data);

	// TODO: execute the lambda expression on the child vector
}

static unique_ptr<FunctionData> ListTransformBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {

	// the list column and the lambda function
	D_ASSERT(bound_function.arguments.size() == 2);
	D_ASSERT(arguments.size() == 2);

	if (arguments[0]->return_type.id() == LogicalTypeId::SQLNULL) {
		bound_function.arguments[0] = LogicalType::SQLNULL;
		bound_function.return_type = LogicalType::SQLNULL;
		return make_unique<VariableReturnBindData>(bound_function.return_type);
	}

	D_ASSERT(LogicalTypeId::LIST == arguments[0]->return_type.id());

	// elements in the transformed lists have the same return type as the lambda function
	bound_function.return_type = LogicalType::LIST(arguments[1]->return_type);

	// TODO:
	// this is about finding out what the input and output types are
	// also, remove the lambda function from the arguments here
	// and add it to the bind info instead

	// should be removed from the bound_function

	// TODO: return custom bind data
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

ScalarFunction ListTransformFun::GetFunction() {
	// TODO: what logical type is the lambda function?
	// after the bind this is any, because it is the return value of the rhs?
	return ScalarFunction({LogicalType::LIST(LogicalType::ANY), LogicalType::ANY},
	                      LogicalType::LIST(LogicalType::ANY), ListTransformFunction, false, false, ListTransformBind,
	                      nullptr);
}

void ListTransformFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_transform", "array_transform"}, GetFunction());
}

} // namespace duckdb