//---------------------------------------------------------------------------
//	@filename:
//		Cascade.h
//
//	@doc:
//		Entry point to Cascade optimizer
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef CASCADE_H
#define CASCADE_H

#include "duckdb/common/types.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/optimizer/cascade/search/CSearchStage.h"
#include "duckdb/optimizer/join_order/cardinality_estimator.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
class Cascade {
public:
	explicit Cascade(ClientContext &context) : context(context), cardinality_estimator(context) {
	}

	duckdb::unique_ptr<PhysicalOperator> Optimize(duckdb::unique_ptr<LogicalOperator> plan);

private:
	ClientContext &context;
	CardinalityEstimator cardinality_estimator;
};
} // namespace duckdb
#endif