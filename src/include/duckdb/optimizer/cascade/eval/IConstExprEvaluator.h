//---------------------------------------------------------------------------
//	@filename:
//		IConstExprEvaluator.h
//
//	@doc:
//		Interface for constant expression evaluator in the optimizer
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPOPT_IConstExprEvaluator_H
#define GPOPT_IConstExprEvaluator_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CRefCount.h"

namespace gpopt
{
using namespace gpos;

	// forward declaration

//---------------------------------------------------------------------------
//	@class:
//		IConstExprEvaluator
//
//	@doc:
//		Interface to access the underlying evaluator of constant expressions
//		(expressions that can be evaluated independent of the contents of the
//		database)
//
//---------------------------------------------------------------------------
class IConstExprEvaluator : public CRefCount
{
public:
	// dtor
	virtual ~IConstExprEvaluator()
	{
	}

	// evaluate the given expression and return the result as a new expression
	// caller takes ownership of returned expression
	virtual CExpression *PexprEval(CExpression *pexpr) = 0;

	// returns true iff the evaluator can evaluate constant expressions without subqueries
	virtual BOOL FCanEvalExpressions() = 0;
};
}  // namespace gpopt

#endif