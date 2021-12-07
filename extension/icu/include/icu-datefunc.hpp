//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-datefunc.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "icu-collate.hpp"

#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct ICUDateFunc {
	using CalendarPtr = unique_ptr<icu::Calendar>;

	struct BindData : public FunctionData {
		explicit BindData(ClientContext &context);
		BindData(const BindData &other);

		CalendarPtr calendar;

		unique_ptr<FunctionData> Copy() override;
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, ScalarFunction &bound_function,
	                                     vector<unique_ptr<Expression>> &arguments);

	template <typename TA, typename TB, typename TR, typename OP>
	static void ExecuteBinary(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.ColumnCount() == 2);

		auto &func_expr = (BoundFunctionExpression &)state.expr;
		auto &info = (BindData &)*func_expr.bind_info;
		CalendarPtr calendar(info.calendar->clone());

		BinaryExecutor::Execute<TA, TB, TR>(args.data[0], args.data[1], result, args.size(), [&](TA left, TB right) {
			return OP::template Operation<TA, TB, TR>(left, right, calendar.get());
		});
	}

	template <typename TA, typename TB, typename TR, typename OP>
	inline static ScalarFunction GetBinaryDateFunction(const LogicalTypeId &left_type, const LogicalTypeId &right_type,
	                                                   const LogicalTypeId &result_type) {
		return ScalarFunction({left_type, right_type}, result_type, ExecuteBinary<TA, TB, timestamp_t, OP>, false,
		                      Bind);
	}
};

} // namespace duckdb
