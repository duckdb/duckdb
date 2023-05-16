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
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/join_order/cardinality_estimator.hpp"

namespace duckdb {
        class Cascade {
        public:
                explicit Cascade(ClientContext &context)
                : context(context), cardinality_estimator(context) {
                }

                unique_ptr<PhysicalOperator> Optimize(unique_ptr<LogicalOperator> plan);
        private:
                ClientContext &context;
                CardinalityEstimator cardinality_estimator;
        };
}

#endif