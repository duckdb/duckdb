#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class Optimizer;
class BoundColumnRefExpression;
class ClientContext;

class UnifiedStringDictionaryOptimizer {
public:
	explicit UnifiedStringDictionaryOptimizer(Optimizer *optimizer, optional_ptr<LogicalOperator> root) {
		this->optimizer = optimizer;
		this->root = root;
	}

	unique_ptr<LogicalOperator> CheckIfUnifiedStringDictionaryRequired(unique_ptr<LogicalOperator> op);
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> op);

private:
	optional_ptr<LogicalOperator> root;

	Optimizer *optimizer;
	vector<optional_ptr<LogicalOperator>> candidate_data_sources;
	vector<optional_ptr<LogicalOperator>> chosen_data_sources;

	unique_ptr<LogicalOperator> prev_op;

	void InsertUnifiedStringDictionaryOperator(optional_ptr<LogicalOperator> op);
	void AddMarkedDataSources();
	bool IsTargetOperator(optional_ptr<LogicalOperator> op);
};

} // namespace duckdb
