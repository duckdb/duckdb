//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/aggregate_rewrite.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/optimizer_type.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

class BoundAggregateExpression;
class ClientContext;
class LogicalAggregate;
class Optimizer;

enum class AggregateRewriteMode : uint8_t { DIRECT, MULTI_STAGE };

//! One aggregate stage. Expressions consume either the rewrite input or the preceding stage.
struct DUCKDB_API AggregateRewriteStage {
	AggregateRewriteStage(TableIndex group_index, TableIndex aggregate_index, vector<unique_ptr<Expression>> groups,
	                      vector<unique_ptr<Expression>> aggregates);

	TableIndex group_index;
	TableIndex aggregate_index;
	vector<unique_ptr<Expression>> groups;
	vector<unique_ptr<Expression>> aggregates;
};

//! A linear aggregate plan and the scalar expression that reconstructs the original result.
struct DUCKDB_API AggregateRewritePlan {
	explicit AggregateRewritePlan(OptimizerType optimizer_type);

	OptimizerType optimizer_type;
	vector<AggregateRewriteStage> stages;
	unique_ptr<Expression> result;
};

struct DUCKDB_API AggregateRewriteInput {
	AggregateRewriteInput(ClientContext &context, const BoundAggregateExpression &aggregate, AggregateRewriteMode mode);
	AggregateRewriteInput(Optimizer &optimizer, const LogicalAggregate &op, const BoundAggregateExpression &aggregate,
	                      AggregateRewriteMode mode);

	ClientContext &context;
	optional_ptr<Optimizer> optimizer;
	optional_ptr<const LogicalAggregate> op;
	const BoundAggregateExpression &aggregate;
	AggregateRewriteMode mode;
};

struct DUCKDB_API AggregateRewriteResult {
	static unique_ptr<AggregateRewriteResult> Direct(unique_ptr<Expression> expression);
	static unique_ptr<AggregateRewriteResult> MultiStage(unique_ptr<AggregateRewritePlan> plan);

	unique_ptr<Expression> expression;
	unique_ptr<AggregateRewritePlan> plan;
};

//! Applies a direct aggregate callback and returns its replacement expression, if any.
DUCKDB_API unique_ptr<Expression> TryDirectAggregateRewrite(AggregateRewriteInput &input);

struct DUCKDB_API FrequencyAggregateFinalizeInput {
	FrequencyAggregateFinalizeInput(AggregateRewriteInput &rewrite_input, TableIndex aggregate_index,
	                                unique_ptr<Expression> value, unique_ptr<Expression> frequency,
	                                unique_ptr<Expression> filter, unique_ptr<Expression> order_key);

	AggregateRewriteInput &rewrite_input;
	TableIndex aggregate_index;
	unique_ptr<Expression> value;
	unique_ptr<Expression> frequency;
	unique_ptr<Expression> filter;
	unique_ptr<Expression> order_key;
};

struct DUCKDB_API FrequencyAggregateFinalizeResult {
	vector<unique_ptr<Expression>> aggregates;
	unique_ptr<Expression> result;
};

typedef FrequencyAggregateFinalizeResult (*frequency_aggregate_finalize_t)(FrequencyAggregateFinalizeInput &input);

//! Builds the shared relational frequency stage and delegates the terminal aggregates and projection to the function.
struct DUCKDB_API FrequencyAggregateRewrite {
	static unique_ptr<AggregateRewriteResult> Create(AggregateRewriteInput &input, bool ignore_nulls, bool retain_order,
	                                                 frequency_aggregate_finalize_t finalize);
};

} // namespace duckdb
