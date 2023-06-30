//---------------------------------------------------------------------------
//	@filename:
//		CExpression.h
//
//	@doc:
//		Basic tree/DAG-based representation for an expression
//---------------------------------------------------------------------------
#ifndef GPOPT_CExpression_H
#define GPOPT_CExpression_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CDynamicPtrArray.h"
#include "duckdb/optimizer/cascade/common/CRefCount.h"
#include "duckdb/optimizer/cascade/base/CColRef.h"
#include "duckdb/optimizer/cascade/base/CCostContext.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropRelational.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropScalar.h"
#include "duckdb/optimizer/cascade/base/CKeyCollection.h"
#include "duckdb/optimizer/cascade/base/CPrintPrefix.h"
#include "duckdb/optimizer/cascade/base/CReqdProp.h"
#include "duckdb/optimizer/cascade/base/CReqdPropRelational.h"
#include "duckdb/optimizer/cascade/cost/CCost.h"
#include "duckdb/optimizer/cascade/metadata/CTableDescriptor.h"
#include "duckdb/optimizer/cascade/operators/COperator.h"
#include "duckdb/optimizer/cascade/statistics/CStatistics.h"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/execution/physical_operator.hpp"

// infinite plan cost
#define GPOPT_INFINITE_COST CCost(1e+100)

// invalid cost value
#define GPOPT_INVALID_COST CCost(-0.5)

namespace gpopt
{

using namespace duckdb;

// cleanup function for arrays

typedef CDynamicPtrArray<unique_ptr<Expression>, CleanupRelease> ExpressionArray;
typedef CDynamicPtrArray<unique_ptr<LogicalOperator>, CleanupRelease> LogicalOperatorArray;
typedef CDynamicPtrArray<unique_ptr<PhysicalOperator>, CleanupRelease> LogicalOperatorArray;
// array of arrays of expression pointers
typedef CDynamicPtrArray<ExpressionArray, CleanupRelease> ExpressionArrays;
}

#endif