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
Expression* CConstExprEvaluatorDefault::PexprEval(Expression* pexpr)
{
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