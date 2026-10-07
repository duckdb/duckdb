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
class BaseStatistics;

enum class AggregateRewriteSourceType : uint8_t { INPUT, STAGE };

//! An input edge in an aggregate rewrite plan.
struct AggregateRewriteSource {
	DUCKDB_API static AggregateRewriteSource Input();
	DUCKDB_API static AggregateRewriteSource Stage(idx_t stage_index);

	bool operator==(const AggregateRewriteSource &other) const;

	AggregateRewriteSourceType type;
	idx_t stage_index;
};

//! One topologically ordered aggregate stage. Original group keys form the prefix of every stage's groups.
struct AggregateRewriteStage {
	DUCKDB_API AggregateRewriteStage(TableIndex group_index, TableIndex aggregate_index,
	                                 vector<AggregateRewriteSource> sources, vector<unique_ptr<Expression>> groups,
	                                 vector<unique_ptr<Expression>> aggregates);

	TableIndex group_index;
	TableIndex aggregate_index;
	vector<AggregateRewriteSource> sources;
	vector<unique_ptr<Expression>> groups;
	vector<unique_ptr<Expression>> aggregates;
};

//! An aggregate DAG. The result expression may reference the output of result_stage.
struct AggregateRewritePlan {
	vector<AggregateRewriteStage> stages;
	idx_t result_stage = DConstants::INVALID_INDEX;
	unique_ptr<Expression> result;
};

struct AggregateRewriteInput {
	DUCKDB_API AggregateRewriteInput(ClientContext &context, const BoundAggregateExpression &aggregate);
	DUCKDB_API AggregateRewriteInput(Optimizer &optimizer, const LogicalAggregate &op,
	                                 const BoundAggregateExpression &aggregate);

	ClientContext &context;
	optional_ptr<Optimizer> optimizer;
	optional_ptr<const LogicalAggregate> op;
	const BoundAggregateExpression &aggregate;
};

//! Statistics exposed to a cost-based aggregate rewrite callback.
struct AggregateRewriteCostInput {
	DUCKDB_API AggregateRewriteCostInput(AggregateRewriteInput &rewrite_input, optional_idx input_cardinality,
	                                     vector<optional_ptr<const BaseStatistics>> argument_statistics);

	AggregateRewriteInput &rewrite_input;
	optional_idx input_cardinality;
	vector<optional_ptr<const BaseStatistics>> argument_statistics;
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
	DUCKDB_API static unique_ptr<AggregateRewritePlan>
	Create(AggregateRewriteInput &input, bool ignore_nulls, bool retain_order, frequency_aggregate_finalize_t finalize);
};

} // namespace duckdb
