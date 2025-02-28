//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/expression_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

class ExpressionFilter : public TableFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::EXPRESSION_FILTER;

public:
	ExpressionFilter(unique_ptr<Expression> expr);

	//! The expression to evaluate
	unique_ptr<Expression> expr;

public:
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) override;
	string ToString(const string &column_name) const override;
	bool Equals(const TableFilter &other) const override;
	unique_ptr<TableFilter> Copy() const override;
	unique_ptr<Expression> ToExpression(const Expression &column) const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
