//---------------------------------------------------------------------------
//	@filename:
//		CNormalizer.h
//
//	@doc:
//		Normalization of expression trees;
//		this currently includes predicates push down
//---------------------------------------------------------------------------
#ifndef GPOPT_CNormalizer_H
#define GPOPT_CNormalizer_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CExpression.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CNormalizer
//
//	@doc:
//		Normalization of expression trees
//
//---------------------------------------------------------------------------
class CNormalizer
{
private:
	// definition of push through function
	typedef void(FnPushThru)(CMemoryPool *mp, CExpression *pexprLogical,
							 CExpression *pexprConj,
							 CExpression **ppexprResult);

	//---------------------------------------------------------------------------
	//	@struct:
	//		SPushThru
	//
	//	@doc:
	//		Mapping of a logical operator to a push through function
	//
	//---------------------------------------------------------------------------
	struct SPushThru
	{
		// logical operator id
		COperator::EOperatorId m_eopid;

		// pointer to push through function
		FnPushThru *m_pfnpt;

	};	// struct SPushThru

	// array of mappings
	static const SPushThru m_rgpt[];

	//  return true if second expression is a child of first expression
	static BOOL FChild(CExpression *pexpr, CExpression *pexprChild);

	// simplify outer joins
	static BOOL FSimplifySelectOnOuterJoin(CMemoryPool *mp,
										   CExpression *pexprOuterJoin,
										   CExpression *pexprPred,
										   CExpression **ppexprResult);

	static BOOL FSimplifySelectOnFullJoin(CMemoryPool *mp,
										  CExpression *pexprFullJoin,
										  CExpression *pexprPred,
										  CExpression **ppexprResult);

	// call normalizer recursively on expression children
	static CExpression *PexprRecursiveNormalize(CMemoryPool *mp,
												CExpression *pexpr);

	// check if a scalar predicate can be pushed through a logical expression
	static BOOL FPushable(CExpression *pexprLogical, CExpression *pexprPred);

	// check if a scalar predicate can be pushed through the child of a sequence project expression
	static BOOL FPushableThruSeqPrjChild(CExpression *pexprSeqPrj,
										 CExpression *pexprPred);

	// check if a conjunct should be pushed through expression's outer child
	static BOOL FPushThruOuterChild(CExpression *pexprLogical);

	// return a Select expression, if needed, with a scalar condition made of given array of conjuncts
	static CExpression *PexprSelect(CMemoryPool *mp, CExpression *pexpr,
									CExpressionArray *pdrgpexpr);

	// push scalar expression through an expression with unary operator with scalar child
	static void PushThruUnaryWithScalarChild(CMemoryPool *mp,
											 CExpression *pexprLogical,
											 CExpression *pexprConj,
											 CExpression **ppexprResult);

	// push scalar expression through a sequence project expression
	static void PushThruSeqPrj(CMemoryPool *mp, CExpression *pexprSeqPrj,
							   CExpression *pexprConj,
							   CExpression **ppexprResult);

	// push scalar expression through a set operation
	static void PushThruSetOp(CMemoryPool *mp, CExpression *pexprSetOp,
							  CExpression *pexprConj,
							  CExpression **ppexprResult);

	// push a conjunct through a CTE anchor operator
	static void PushThruUnaryWithoutScalarChild(CMemoryPool *mp,
												CExpression *pexprLogical,
												CExpression *pexprConj,
												CExpression **ppexprResult);

	// push scalar expression through a select expression
	static void PushThruSelect(CMemoryPool *mp, CExpression *pexprSelect,
							   CExpression *pexprConj,
							   CExpression **ppexprResult);

	// split the given conjunct into pushable and unpushable predicates
	static void SplitConjunct(CMemoryPool *mp, CExpression *pexpr,
							  CExpression *pexprConj,
							  CExpressionArray **ppdrgpexprPushable,
							  CExpressionArray **ppdrgpexprUnpushable);

	// split the given conjunct into pushable and unpushable predicates for a sequence project expression
	static void SplitConjunctForSeqPrj(CMemoryPool *mp,
									   CExpression *pexprSeqPrj,
									   CExpression *pexprConj,
									   CExpressionArray **ppdrgpexprPushable,
									   CExpressionArray **ppdrgpexprUnpushable);

	// push scalar expression through left outer join children
	static void PushThruOuterChild(CMemoryPool *mp, CExpression *pexpr,
								   CExpression *pexprConj,
								   CExpression **ppexprResult);

	// push scalar expression through a join expression
	static void PushThruJoin(CMemoryPool *mp, CExpression *pexprJoin,
							 CExpression *pexprConj,
							 CExpression **ppexprResult);

	// push scalar expression through logical expression
	static void PushThru(CMemoryPool *mp, CExpression *pexprLogical,
						 CExpression *pexprConj, CExpression **ppexprResult);

	// push an array of conjuncts through logical expression, and compute an array of remaining conjuncts
	static void PushThru(CMemoryPool *mp, CExpression *pexprLogical,
						 CExpressionArray *pdrgpexprConjuncts,
						 CExpression **ppexprResult,
						 CExpressionArray **ppdrgpexprRemaining);

	// private copy ctor
	CNormalizer(const CNormalizer &);

	// pull logical projects as far up the logical tree as possible, and
	// combine consecutive projects if possible
	static CExpression *PexprPullUpAndCombineProjects(CMemoryPool *mp,
													  CExpression *pexpr,
													  BOOL *pfSuccess);

	// pull up project elements from the given projection expression that do not
	// exist in the given used columns set
	static CExpression *PexprPullUpProjectElements(
		CMemoryPool *mp, CExpression *pexpr, CColRefSet *pcrsUsed,
		CColRefSet *pcrsOutput, CExpressionArray **ppdrgpexprPrElPullUp);

#ifdef GPOS_DEBUG
	// check if the columns used by the operator are a subset of its input columns
	static BOOL FLocalColsSubsetOfInputCols(CMemoryPool *mp,
											CExpression *pexpr);
#endif	//GPOS_DEBUG

public:
	// main driver
	static CExpression *PexprNormalize(CMemoryPool *mp, CExpression *pexpr);

	// normalize joins so that they are on colrefs
	static CExpression *PexprNormalizeJoins(CMemoryPool *mp,
											CExpression *pexpr);

	// pull logical projects as far up the logical tree as possible, and
	// combine consecutive projects if possible
	static CExpression *PexprPullUpProjections(CMemoryPool *mp,
											   CExpression *pexpr);

};	// class CNormalizer
}  // namespace gpopt

#endif