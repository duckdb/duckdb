#include "core_functions/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

namespace {

struct ProductState {
	bool empty;
	double val;
};

struct ProductReduce {
	template <class T>
	static T Operation(T left, T right) {
		return MultiplyOperator::template Operation<T, T, T>(left, right);
	}
};

using ProductFunction = EmptyValAggregate<ProductReduce, ConstantInit<1>>;

LogicalType GetProductStateType(const AggregateFunction &function) {
	child_list_t<LogicalType> children;
	children.emplace_back("empty", LogicalType::BOOLEAN);
	children.emplace_back("val", LogicalType::DOUBLE);
	return LogicalType::STRUCT(std::move(children));
}

} // namespace

AggregateFunction ProductFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<ProductState, double, double, ProductFunction>(
	           LogicalType(LogicalTypeId::DOUBLE), LogicalType::DOUBLE)
	    .SetStructStateExport(GetProductStateType);
}

} // namespace duckdb
