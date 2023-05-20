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
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/optimizer/join_order/cardinality_estimator.hpp"
#include "duckdb/optimizer/cascade/search/CSearchStage.h"
#include "duckdb/optimizer/cascade/operators/CExpression.h"

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

                gpopt::CSearchStageArray* LoadSearchStrategy(CMemoryPool* mp);

                gpopt::CExpression* Orca2Duck(unique_ptr<LogicalOperator> plan);

                unique_ptr<PhysicalOperator> Duck2Orca(gpopt::CExpression* Plan);
        };
}

#endif