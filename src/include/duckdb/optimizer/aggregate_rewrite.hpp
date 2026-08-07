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
struct AggregateRewriteStage {
	DUCKDB_API AggregateRewriteStage(TableIndex group_index, TableIndex aggregate_index,
	                                 vector<unique_ptr<Expression>> groups, vector<unique_ptr<Expression>> aggregates);

	TableIndex group_index;
	TableIndex aggregate_index;
	vector<unique_ptr<Expression>> groups;
	vector<unique_ptr<Expression>> aggregates;
};

//! A linear aggregate plan and the scalar expression that reconstructs the original result.
struct AggregateRewritePlan {
	DUCKDB_API explicit AggregateRewritePlan(OptimizerType optimizer_type);

	OptimizerType optimizer_type;
	vector<AggregateRewriteStage> stages;
	unique_ptr<Expression> result;
};

struct AggregateRewriteInput {
	DUCKDB_API AggregateRewriteInput(ClientContext &context, const BoundAggregateExpression &aggregate,
	                                 AggregateRewriteMode mode);
	DUCKDB_API AggregateRewriteInput(Optimizer &optimizer, const LogicalAggregate &op,
	                                 const BoundAggregateExpression &aggregate, AggregateRewriteMode mode);

	ClientContext &context;
	optional_ptr<Optimizer> optimizer;
	optional_ptr<const LogicalAggregate> op;
	const BoundAggregateExpression &aggregate;
	AggregateRewriteMode mode;
};

struct AggregateRewriteResult {
	DUCKDB_API static unique_ptr<AggregateRewriteResult> Direct(unique_ptr<Expression> expression);
	DUCKDB_API static unique_ptr<AggregateRewriteResult> MultiStage(unique_ptr<AggregateRewritePlan> plan);

	unique_ptr<Expression> expression;
	unique_ptr<AggregateRewritePlan> plan;
};

//! Applies a direct aggregate callback and returns its replacement expression, if any.
DUCKDB_API unique_ptr<Expression> TryDirectAggregateRewrite(AggregateRewriteInput &input);

struct FrequencyAggregateFinalizeInput {
	DUCKDB_API FrequencyAggregateFinalizeInput(AggregateRewriteInput &rewrite_input, TableIndex aggregate_index,
	                                           unique_ptr<Expression> value, unique_ptr<Expression> frequency,
	                                           unique_ptr<Expression> filter, unique_ptr<Expression> order_key);

	AggregateRewriteInput &rewrite_input;
	TableIndex aggregate_index;
	unique_ptr<Expression> value;
	unique_ptr<Expression> frequency;
	unique_ptr<Expression> filter;
	unique_ptr<Expression> order_key;
};

struct FrequencyAggregateFinalizeResult {
	vector<unique_ptr<Expression>> aggregates;
	unique_ptr<Expression> result;
};

typedef FrequencyAggregateFinalizeResult (*frequency_aggregate_finalize_t)(FrequencyAggregateFinalizeInput &input);

//! Builds the shared relational frequency stage and delegates the terminal aggregates and projection to the function.
struct FrequencyAggregateRewrite {
	DUCKDB_API static unique_ptr<AggregateRewriteResult>
	Create(AggregateRewriteInput &input, bool ignore_nulls, bool retain_order, frequency_aggregate_finalize_t finalize);
};

} // namespace duckdb
