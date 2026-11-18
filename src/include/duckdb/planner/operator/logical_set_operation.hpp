//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_set_operation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/joinside.hpp"

namespace duckdb {
class Binder;

class LogicalSetOperation : public LogicalOperator {
	LogicalSetOperation(TableIndex table_index, idx_t column_count, LogicalOperatorType type, bool setop_all,
	                    bool allow_out_of_order);

public:
	LogicalPlanSQLExportResult ToSQL(LogicalPlanSQLExportContext &context,
	                                 const LogicalPlanVerificationPath &path) override;

	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_INVALID;

public:
	LogicalSetOperation(TableIndex table_index, idx_t column_count, unique_ptr<LogicalOperator> top,
	                    unique_ptr<LogicalOperator> bottom, LogicalOperatorType type, bool setop_all,
	                    bool allow_out_of_order = true);
	LogicalSetOperation(TableIndex table_index, idx_t column_count, vector<unique_ptr<LogicalOperator>> children,
	                    LogicalOperatorType type, bool setop_all, bool allow_out_of_order = true);

	TableIndex table_index;
	idx_t column_count;
	bool setop_all;
	//! Whether or not UNION statements can be executed out of order
	bool allow_out_of_order;
	//! EXCEPT / INTERSECT: the types the binder resolved for the columns. The columns are compared as these types,
	//! that is under their collations. The children do not tell: a UNION child has the types of its first child, and
	//! a cast to VARCHAR does not carry a collation
	vector<LogicalType> comparison_types;
	//! EXCEPT / INTERSECT: how many of the last columns are the correlated columns of a subquery that the set operation
	//! is part of (FlattenDependentJoins appends them). They are compared as they are, under no collation: the join
	//! with the outer query compares them so, and a collation would mix the rows of outer values it treats as equal
	idx_t correlated_column_count = 0;

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return GenerateColumnBindings(table_index, column_count);
	}

	//! The types the columns of an EXCEPT / INTERSECT are compared as: comparison_types, or for a set operation that
	//! was not planned by the binder what the children say (a collation of either child applies)
	vector<LogicalType> GetComparisonTypes(ClientContext &context);
	//! How an EXCEPT / INTERSECT compares the columns of its children: IS NOT DISTINCT FROM (NULLs are equal) on every
	//! column, under its collation except for the correlated columns. These are the conditions of the join CreateJoin
	//! builds, without the row numbers.
	vector<JoinCondition> GetComparisons(ClientContext &context);
	//! Plans EXCEPT as an ANTI join and INTERSECT as a SEMI join on all columns. With setop_all both children get a
	//! row_number() over all columns that is compared as well, so that the n-th copies of a row match each other.
	//! The last correlated_column_count columns are compared as they are, the others under their collations.
	//! The result is a projection of the left columns with the table index of the set operation.
	static unique_ptr<LogicalOperator> CreateJoin(Binder &binder, const vector<LogicalType> &types,
	                                              TableIndex setop_index, LogicalOperatorType type, bool setop_all,
	                                              unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right,
	                                              idx_t correlated_column_count = 0);

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	vector<TableIndex> GetTableIndex() const override;
	string GetName() const override;

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};
} // namespace duckdb
