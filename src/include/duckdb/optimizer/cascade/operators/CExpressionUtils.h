//---------------------------------------------------------------------------
//	@filename:
//		CExpressionUtils.h
//
//	@doc:
//		Utility routines for transforming expressions
//
//	@owner:
//		,
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CExpressionUtils_H
#define GPOPT_CExpressionUtils_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CExpression.h"

namespace gpopt
{
using namespace gpos;

// fwd declarations
class CExpression;

//---------------------------------------------------------------------------
//	@class:
//		CExpressionUtils
//
//	@doc:
//		Utility routines for transforming expressions
//
//---------------------------------------------------------------------------
class CExpressionUtils
{
private:
	// unnest a given expression's child and append unnested nodes to given array
	static void UnnestChild(CMemoryPool *mp, CExpression *pexpr,
							ULONG UlChildIndex, BOOL fAnd, BOOL fOr,
							BOOL fNotChildren, CExpressionArray *pdrgpexpr);

	// append the unnested children of given expression to given array
	static void AppendChildren(CMemoryPool *mp, CExpression *pexpr,
							   CExpressionArray *pdrgpexpr);

	// return an array of expression children after being unnested
	static CExpressionArray *PdrgpexprUnnestChildren(CMemoryPool *mp,
													 CExpression *pexpr);

	// push not expression one level down the given expression
	static CExpression *PexprPushNotOneLevel(CMemoryPool *mp,
											 CExpression *pexpr);

public:
	// remove duplicate AND/OR children
	static CExpression *PexprDedupChildren(CMemoryPool *mp, CExpression *pexpr);

	// unnest AND/OR/NOT predicates
	static CExpression *PexprUnnest(CMemoryPool *mp, CExpression *pexpr);
};
}  // namespace gpopt

#endif