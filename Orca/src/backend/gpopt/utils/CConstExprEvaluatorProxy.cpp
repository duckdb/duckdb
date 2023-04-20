//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CConstExprEvaluatorProxy.cpp
//
//	@doc:
//		Wrapper over GPDB's expression evaluator that takes a constant expression,
//		given as DXL, evaluates it and returns the result as DXL. In case the expression
//		has variables, an exception is raised
//
//	@test:
//
//---------------------------------------------------------------------------

extern "C" {
#include "postgres.h"

#include "executor/executor.h"
}

#include "gpopt/gpdbwrappers.h"
#include "gpopt/translate/CTranslatorScalarToDXL.h"
#include "gpopt/utils/CConstExprEvaluatorProxy.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/exception.h"

using namespace gpdxl;
using namespace gpmd;
using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CConstExprEvaluatorProxy::EmptyMappingColIdVar::PvarFromDXLNodeScId
//
//	@doc:
//		Raises an exception in case someone looks up a variable
//
//---------------------------------------------------------------------------
Var *
CConstExprEvaluatorProxy::CEmptyMappingColIdVar::VarFromDXLNodeScId(
	const CDXLScalarIdent * /*scalar_ident*/
)
{
	elog(LOG,
		 "Expression passed to CConstExprEvaluatorProxy contains variables. "
		 "Evaluation will fail and an exception will be thrown.");
	GPOS_RAISE(gpdxl::ExmaGPDB, gpdxl::ExmiGPDBError);
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstExprEvaluatorProxy::EvaluateExpr
//
//	@doc:
//		Evaluate 'expr', assumed to be a constant expression, and return the DXL representation
// 		of the result. Caller keeps ownership of 'expr' and takes ownership of the returned pointer.
//
//---------------------------------------------------------------------------
CDXLNode *
CConstExprEvaluatorProxy::EvaluateExpr(const CDXLNode *dxl_expr)
{
	// Translate DXL -> GPDB Expr
	Expr *expr = m_dxl2scalar_translator.TranslateDXLToScalar(
		dxl_expr, &m_emptymapcidvar);
	GPOS_ASSERT(NULL != expr);

	// Evaluate the expression
	Expr *result = gpdb::EvaluateExpr(expr, gpdb::ExprType((Node *) expr),
									  gpdb::ExprTypeMod((Node *) expr));

	if (!IsA(result, Const))
	{
#ifdef GPOS_DEBUG
		elog(
			NOTICE,
			"Expression did not evaluate to Const, but to an expression of type %d",
			result->type);
#endif
		GPOS_RAISE(gpdxl::ExmaConstExprEval, gpdxl::ExmiConstExprEvalNonConst);
	}

	Const *const_result = (Const *) result;
	CDXLDatum *datum_dxl = CTranslatorScalarToDXL::TranslateConstToDXL(
		m_mp, m_md_accessor, const_result);
	CDXLNode *dxl_result = GPOS_NEW(m_mp)
		CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLScalarConstValue(m_mp, datum_dxl));
	gpdb::GPDBFree(result);
	gpdb::GPDBFree(expr);

	return dxl_result;
}

// EOF
