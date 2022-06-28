#pragma once

#include "substrait/algebra.pb.h"
#include <string>
#include <unordered_map>
#include "substrait/plan.pb.h"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"

namespace duckdb {
class DuckDBToSubstrait {
public:
	explicit DuckDBToSubstrait(duckdb::LogicalOperator &dop) {
		TransformPlan(dop);
	};

	~DuckDBToSubstrait() {
		plan.Clear();
	}
	//! Serializes the substrait plan to a string
	string SerializeToString();

private:
	//! Transform DuckDB Plan to Substrait Plan
	void TransformPlan(duckdb::LogicalOperator &dop);
	//! Registers a function
	uint64_t RegisterFunction(const std::string &name);
	//! Creates a reference to a table column
	void CreateFieldRef(substrait::Expression *expr, uint64_t col_idx);

	//! Transforms Relation Root
	substrait::RelRoot *TransformRootOp(LogicalOperator &dop);

	//! Methods to Transform Logical Operators to Substrait Relations
	substrait::Rel *TransformOp(duckdb::LogicalOperator &dop);
	substrait::Rel *TransformFilter(duckdb::LogicalOperator &dop);
	substrait::Rel *TransformProjection(duckdb::LogicalOperator &dop);
	substrait::Rel *TransformTopN(duckdb::LogicalOperator &dop);
	substrait::Rel *TransformLimit(duckdb::LogicalOperator &dop);
	substrait::Rel *TransformOrderBy(duckdb::LogicalOperator &dop);
	substrait::Rel *TransformComparisonJoin(duckdb::LogicalOperator &dop);
	substrait::Rel *TransformAggregateGroup(duckdb::LogicalOperator &dop);
	substrait::Rel *TransformGet(duckdb::LogicalOperator &dop);
	substrait::Rel *TransformCrossProduct(duckdb::LogicalOperator &dop);

	//! Methods to transform DuckDBConstants to Substrait Expressions
	void TransformConstant(duckdb::Value &dval, substrait::Expression &sexpr);
	void TransformInteger(duckdb::Value &dval, substrait::Expression &sexpr);
	void TransformDouble(Value &dval, substrait::Expression &sexpr);
	void TransformBigInt(duckdb::Value &dval, substrait::Expression &sexpr);
	void TransformDate(duckdb::Value &dval, substrait::Expression &sexpr);
	void TransformVarchar(duckdb::Value &dval, substrait::Expression &sexpr);
	void TransformBoolean(duckdb::Value &dval, substrait::Expression &sexpr);
	void TransformDecimal(duckdb::Value &dval, substrait::Expression &sexpr);
	void TransformHugeInt(Value &dval, substrait::Expression &sexpr);
	//! Methods to transform a DuckDB Expression to a Substrait Expression
	void TransformExpr(duckdb::Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset = 0);
	void TransformBoundRefExpression(duckdb::Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset);
	void TransformCastExpression(duckdb::Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset);
	void TransformFunctionExpression(duckdb::Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset);
	void TransformConstantExpression(duckdb::Expression &dexpr, substrait::Expression &sexpr);
	void TransformComparisonExpression(duckdb::Expression &dexpr, substrait::Expression &sexpr);
	void TransformConjunctionExpression(duckdb::Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset);
	void TransformNotNullExpression(duckdb::Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset);
	void TransformCaseExpression(duckdb::Expression &dexpr, substrait::Expression &sexpr);
	void TransformInExpression(duckdb::Expression &dexpr, substrait::Expression &sexpr);

	//! Transforms a DuckDB Logical Type into a Substrait Type
	::substrait::Type DuckToSubstraitType(LogicalType &d_type);
	//! Methods to transform DuckDB Filters to Substrait Expression
	substrait::Expression *TransformFilter(uint64_t col_idx, duckdb::TableFilter &dfilter);
	substrait::Expression *TransformIsNotNullFilter(uint64_t col_idx, duckdb::TableFilter &dfilter);
	substrait::Expression *TransformConjuctionAndFilter(uint64_t col_idx, duckdb::TableFilter &dfilter);
	substrait::Expression *TransformConstantComparisonFilter(uint64_t col_idx, duckdb::TableFilter &dfilter);

	//! Transforms DuckDB Join Conditions to Substrait Expression
	substrait::Expression *TransformJoinCond(duckdb::JoinCondition &dcond, uint64_t left_ncol);
	//! Transforms DuckDB Sort Order to Substrait Sort Order
	void TransformOrder(duckdb::BoundOrderByNode &dordf, substrait::SortField &sordf);

	string GetDecimalInternalString(duckdb::Value &value);

	//! Creates a Conjuction
	template <typename T, typename FUNC>
	substrait::Expression *CreateConjunction(T &source, FUNC f) {
		substrait::Expression *res = nullptr;
		for (auto &ele : source) {
			auto child_expression = f(ele);
			if (!res) {
				res = child_expression;
			} else {
				auto temp_expr = new substrait::Expression();
				auto scalar_fun = temp_expr->mutable_scalar_function();
				scalar_fun->set_function_reference(RegisterFunction("and"));
				scalar_fun->mutable_args()->AddAllocated(res);
				scalar_fun->mutable_args()->AddAllocated(child_expression);
				res = temp_expr;
			}
		}
		return res;
	}

	//! Variables used to register functions
	std::unordered_map<std::string, uint64_t> functions_map;
	uint64_t last_function_id = 0;

	//! The substrait Plan
	substrait::Plan plan;
};
} // namespace duckdb
