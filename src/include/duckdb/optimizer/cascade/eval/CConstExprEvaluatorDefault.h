//---------------------------------------------------------------------------
//	@filename:
//		CConstExprEvaluatorDefault.h
//
//	@doc:
//		Dummy implementation of the constant expression evaluator
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CConstExprEvaluatorDefault_H
#define GPOPT_CConstExprEvaluatorDefault_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/eval/IConstExprEvaluator.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CConstExprEvaluatorDefault
//
//	@doc:
//		Constant expression evaluator default implementation for the case when
//		no database instance is available
//
//---------------------------------------------------------------------------
class CConstExprEvaluatorDefault : public IConstExprEvaluator
{
private:
	// private copy ctor
	CConstExprEvaluatorDefault(const CConstExprEvaluatorDefault &);

public:
	// ctor
	CConstExprEvaluatorDefault() : IConstExprEvaluator()
	{
	}

	// dtor
	virtual ~CConstExprEvaluatorDefault();

	// Evaluate the given expression and return the result as a new expression
	virtual Expression* PexprEval(Expression* pexpr);

	// Returns true iff the evaluator can evaluate constant expressions
	virtual bool FCanEvalExpressions();
};
}  // namespace gpopt
#endif