#include "duckdb/planner/expression/bound_window_function.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

namespace duckdb {

BoundWindowFunction::BoundWindowFunction(WindowFunction function, vector<unique_ptr<Expression>> children,
                                         unique_ptr<FunctionData> bind_info)
    : Expression(function.window_enum, ExpressionClass::BOUND_WINDOW_FUNCTION, function.GetReturnType()),
      function(std::move(function)), children(std::move(children)), bind_info(std::move(bind_info)) {
	D_ASSERT(!this->function.name.empty());
}

string BoundWindowFunction::ToString() const {
	return FunctionExpression::ToString<BoundWindowFunction, Expression, BoundOrderModifier>(
	    *this, string(), string(), function.name, false, IsDistinct(), filter.get(), order_bys.get());
}

unique_ptr<Expression> BoundWindowFunction::Copy() const {
	vector<unique_ptr<Expression>> new_children;
	new_children.reserve(children.size());
	for (auto &child : children) {
		new_children.push_back(child->Copy());
	}
	auto new_bind_info = bind_info ? bind_info->Copy() : nullptr;
	auto copy = make_uniq<BoundWindowFunction>(function, std::move(new_children), std::move(new_bind_info));
	copy->CopyProperties(*this);

	copy->aggr_type = aggr_type;
	copy->filter = filter ? filter->Copy() : nullptr;
	copy->order_bys = order_bys ? order_bys->Copy() : nullptr;
	copy->arg_order_bys = arg_order_bys ? arg_order_bys->Copy() : nullptr;
	return std::move(copy);
}

} // namespace duckdb
