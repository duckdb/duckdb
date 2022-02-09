#pragma once

#include "substrait/expression.pb.h"
#include <string>
#include <unordered_map>
#include "substrait/plan.pb.h"
#include "substrait/relations.pb.h"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"

namespace duckdb {
class DuckDBToSubstrait {
public:
	DUCKDB_API explicit DuckDBToSubstrait(duckdb::LogicalOperator &dop) {
		TransformPlan(dop);
	};

	DUCKDB_API ~DuckDBToSubstrait() {
		plan.Clear();
	}
	//! Serializes the substrait plan to a string
	string SerializeToString();

private:
	//! Transform DuckDB Plan to Substrait Plan
	DUCKDB_API void TransformPlan(duckdb::LogicalOperator &dop);
	//! Registers a function
	DUCKDB_API uint64_t RegisterFunction(const std::string &name);
	//! Creates a reference to a table column
	DUCKDB_API void CreateFieldRef(substrait::Expression *expr, uint64_t col_idx);

	//! Methods to Transform Logical Operators to Substrait Relations
	DUCKDB_API substrait::Rel *TransformOp(duckdb::LogicalOperator &dop);
	DUCKDB_API substrait::Rel *TransformFilter(duckdb::LogicalOperator &dop);
	DUCKDB_API substrait::Rel *TransformProjection(duckdb::LogicalOperator &dop);
	DUCKDB_API substrait::Rel *TransformTopN(duckdb::LogicalOperator &dop);
	DUCKDB_API substrait::Rel *TransformLimit(duckdb::LogicalOperator &dop);
	DUCKDB_API substrait::Rel *TransformOrderBy(duckdb::LogicalOperator &dop);
	DUCKDB_API substrait::Rel *TransformComparisonJoin(duckdb::LogicalOperator &dop);
	DUCKDB_API substrait::Rel *TransformAggregateGroup(duckdb::LogicalOperator &dop);
	DUCKDB_API substrait::Rel *TransformGet(duckdb::LogicalOperator &dop);
	DUCKDB_API substrait::Rel *TransformCrossProduct(duckdb::LogicalOperator &dop);

	//! Methods to transform DuckDBConstants to Substrait Expressions
	DUCKDB_API void TransformConstant(duckdb::Value &dval, substrait::Expression &sexpr);
	DUCKDB_API void TransformInteger(duckdb::Value &dval, substrait::Expression &sexpr);
	DUCKDB_API void TransformBigInt(duckdb::Value &dval, substrait::Expression &sexpr);
	DUCKDB_API void TransformDate(duckdb::Value &dval, substrait::Expression &sexpr);
	DUCKDB_API void TransformVarchar(duckdb::Value &dval, substrait::Expression &sexpr);
	DUCKDB_API void TransformHugeInt(duckdb::Value &dval, substrait::Expression &sexpr);
	DUCKDB_API void TransformBoolean(duckdb::Value &dval, substrait::Expression &sexpr);
	DUCKDB_API void TransformDecimal(duckdb::Value &dval, substrait::Expression &sexpr);

	//! Methods to transform a DuckDB Expression to a Substrait Expression
	DUCKDB_API void TransformExpr(duckdb::Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset = 0);
	DUCKDB_API void TransformBoundRefExpression(duckdb::Expression &dexpr, substrait::Expression &sexpr,
	                                            uint64_t col_offset);
	DUCKDB_API void TransformCastExpression(duckdb::Expression &dexpr, substrait::Expression &sexpr,
	                                        uint64_t col_offset);
	DUCKDB_API void TransformFunctionExpression(duckdb::Expression &dexpr, substrait::Expression &sexpr,
	                                            uint64_t col_offset);
	DUCKDB_API void TransformConstantExpression(duckdb::Expression &dexpr, substrait::Expression &sexpr);
	DUCKDB_API void TransformComparisonExpression(duckdb::Expression &dexpr, substrait::Expression &sexpr);
	DUCKDB_API void TransformConjunctionExpression(duckdb::Expression &dexpr, substrait::Expression &sexpr,
	                                               uint64_t col_offset);
	DUCKDB_API void TransformNotNullExpression(duckdb::Expression &dexpr, substrait::Expression &sexpr,
	                                           uint64_t col_offset);
	DUCKDB_API void TransformCaseExpression(duckdb::Expression &dexpr, substrait::Expression &sexpr);

	//! Methods to transform DuckDB Filters to Substrait Expression
	DUCKDB_API substrait::Expression *TransformFilter(uint64_t col_idx, duckdb::TableFilter &dfilter);
	DUCKDB_API substrait::Expression *TransformIsNotNullFilter(uint64_t col_idx, duckdb::TableFilter &dfilter);
	DUCKDB_API substrait::Expression *TransformConjuctionAndFilter(uint64_t col_idx, duckdb::TableFilter &dfilter);
	DUCKDB_API substrait::Expression *TransformConstantComparisonFilter(uint64_t col_idx, duckdb::TableFilter &dfilter);

	//! Transforms DuckDB Join Conditions to Substrait Expression
	DUCKDB_API substrait::Expression *TransformJoinCond(duckdb::JoinCondition &dcond, uint64_t left_ncol);
	//! Transforms DuckDB Sort Order to Substrait Sort Order
	DUCKDB_API void TransformOrder(duckdb::BoundOrderByNode &dordf, substrait::SortField &sordf);

	DUCKDB_API string GetDecimalInternalString(duckdb::Value &value);

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