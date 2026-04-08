//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/expression_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {
class ExpressionExecutor;
class BaseStatistics;
class ClientContext;
class Deserializer;
class Serializer;

class ExpressionFilter : public TableFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::EXPRESSION_FILTER;

public:
	explicit ExpressionFilter(unique_ptr<Expression> expr);

	//! The expression to evaluate
	unique_ptr<Expression> expr;

public:
	bool EvaluateWithConstant(ClientContext &context, const Value &val);
	bool EvaluateWithConstant(ExpressionExecutor &executor, const Value &val) const;

	FilterPropagateResult CheckStatistics(BaseStatistics &stats) const override;
	string ToString(const string &column_name) const override;
	bool Equals(const TableFilter &other) const override;
	unique_ptr<TableFilter> Copy() const override;
	unique_ptr<Expression> ToExpression(const Expression &column) const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
	static void ReplaceExpressionRecursive(unique_ptr<Expression> &expr, const Expression &column,
	                                       ExpressionType replace_type = ExpressionType::BOUND_REF);
};

} // namespace duckdb
