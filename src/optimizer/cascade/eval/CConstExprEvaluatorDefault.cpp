//---------------------------------------------------------------------------
//	@filename:
//		CConstExprEvaluatorDefault.cpp
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
#include "duckdb/optimizer/cascade/eval/CConstExprEvaluatorDefault.h"
#include "duckdb/optimizer/cascade/operators/CExpression.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CConstExprEvaluatorDefault::~CConstExprEvaluatorDefault
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CConstExprEvaluatorDefault::~CConstExprEvaluatorDefault()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CConstExprEvaluatorDefault::PexprEval
//
//	@doc:
//		Returns the given expression after having increased its ref count
//
//---------------------------------------------------------------------------
CExpression* CConstExprEvaluatorDefault::PexprEval(CExpression *pexpr)
{
	pexpr->AddRef();
	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstExprEvaluatorDefault::FCanEvalFunctions
//
//	@doc:
//		Returns false, since this evaluator cannot call any functions
//
//---------------------------------------------------------------------------
BOOL CConstExprEvaluatorDefault::FCanEvalExpressions()
{
	return false;
}