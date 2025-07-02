#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/optimizer.hpp"
namespace duckdb {

class Optimizer;
class BoundColumnRefExpression;
class ClientContext;

struct UnifiedStringDictionaryOptimizerContext {
	unique_ptr<LogicalOperator> op;
	bool target_operator_found;
};

class UnifiedStringDictionaryOptimizer {
public:
	explicit UnifiedStringDictionaryOptimizer(Optimizer *optimizer, optional_ptr<LogicalOperator> root) {
		this->optimizer = optimizer;
	}

	unique_ptr<LogicalOperator> CheckIfUnifiedStringDictionaryRequired(unique_ptr<LogicalOperator> op);
	UnifiedStringDictionaryOptimizerContext Rewrite(unique_ptr<LogicalOperator> op);

private:
	Optimizer *optimizer;
	static constexpr idx_t JOIN_CARDINALITY_THRESHOLD = 2000;
	static constexpr idx_t JOIN_CARDINALITY_RATIO_THRESHOLD = 10;

	bool CheckIfTargetOperatorAndInsert(optional_ptr<LogicalOperator> op);
	// Only enable flat vector insertion if joining on primary-foreign key relation.
	bool EnableFlatVecInsertion(optional_ptr<LogicalOperator> op, optional_ptr<LogicalOperator> neighbor_op);
};

} // namespace duckdb
