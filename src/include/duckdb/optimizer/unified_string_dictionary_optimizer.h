#pragma once

#include "duckdb/planner/logical_operator.hpp"

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

	bool CheckIfTargetOperatorAndInsert(optional_ptr<LogicalOperator> op);
};

} // namespace duckdb
