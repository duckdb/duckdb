#pragma once

#include "substrait/expression.pb.h"
#include <string>
#include <unordered_map>
#include "duckdb/common/helper.hpp"
#include "substrait/plan.pb.h"
#include "substrait/relations.pb.h"

using namespace std;

namespace duckdb {
class TableFilter;
class LogicalOperator;
struct BoundOrderByNode;
class Value;
class Expression;
struct JoinCondition;

class DuckDBToSubstrait {
public:
	substrait::Plan plan;
	DuckDBToSubstrait() {};

	~DuckDBToSubstrait() {
		plan.Clear();
	}

	void TransformPlan(duckdb::LogicalOperator &dop);

	void SerializeToString(string &serialized) {
		if (!plan.SerializeToString(&serialized)) {
			throw runtime_error("It was not possible to serialize the substrait plan");
		}
	}

private:
	uint64_t RegisterFunction(std::string name);
	void CreateFieldRef(substrait::Expression *expr, uint64_t col_idx);
	void TransformConstant(duckdb::Value &dval, substrait::Expression &sexpr);
	void TransformExpr(duckdb::Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset = 0);
	substrait::Expression *TransformFilter(uint64_t col_idx, duckdb::TableFilter &dfilter);
	substrait::Expression *TransformJoinCond(duckdb::JoinCondition &dcond, uint64_t left_ncol);
	void TransformOrder(duckdb::BoundOrderByNode &dordf, substrait::SortField &sordf);

	//! Methods to Transform Logical Operators into Substrait Relations
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

	template <typename T, typename Func>
	substrait::Expression *CreateConjunction(T &source, Func f) {
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

	std::unordered_map<std::string, uint64_t> functions_map;
	uint64_t last_function_id = 0;
};
} // namespace duckdb