//---------------------------------------------------------------------------
//	@filename:
//		CExpressionPreprocessor.h
//
//	@doc:
//		Expression tree preprocessing routines, needed to prepare an input
//		logical expression to be optimized
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CExpressionPreprocessor_H
#define GPOPT_CExpressionPreprocessor_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/base/CColumnFactory.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/mdcache/CMDAccessor.h"
#include "duckdb/optimizer/cascade/operators/CExpression.h"
#include "duckdb/optimizer/cascade/operators/CScalarBoolOp.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CExpressionPreprocessor
//
//	@doc:
//		Expression preprocessing routines
//
//---------------------------------------------------------------------------
class CExpressionPreprocessor
{
private:
	// map CTE id to collected predicates
	typedef CHashMap<ULONG, CExpressionArray, gpos::HashValue<ULONG>,
					 gpos::Equals<ULONG>, CleanupDelete<ULONG>,
					 CleanupRelease<CExpressionArray> >
		CTEPredsMap;

	// iterator for map of CTE id to collected predicates
	typedef CHashMapIter<ULONG, CExpressionArray, gpos::HashValue<ULONG>,
						 gpos::Equals<ULONG>, CleanupDelete<ULONG>,
						 CleanupRelease<CExpressionArray> >
		CTEPredsMapIter;

	// generate a conjunction of equality predicates between the columns in the given set
	static CExpression *PexprConjEqualityPredicates(CMemoryPool *mp,
													CColRefSet *pcrs);

	// additional equality predicates are generated based on the equivalence
	// classes in the constraint properties of the expression
	static CExpression *PexprAddEqualityPreds(CMemoryPool *mp,
											  CExpression *pexpr,
											  CColRefSet *pcrsProcessed);

	// check if all columns in the given equivalence class come from one of the
	// children of the given expression
	static BOOL FEquivClassFromChild(CColRefSet *pcrs, CExpression *pexpr);

	// generate predicates for the given set of columns based on the given
	// constraint property
	static CExpression *PexprScalarPredicates(
		CMemoryPool *mp, CPropConstraint *ppc,
		CPropConstraint *constraintsForOuterRefs, CColRefSet *pcrsNotNull,
		CColRefSet *pcrs, CColRefSet *pcrsProcessed);

	// eliminate self comparisons
	static CExpression *PexprEliminateSelfComparison(CMemoryPool *mp,
													 CExpression *pexpr);

	// remove CTE Anchor nodes
	static CExpression *PexprRemoveCTEAnchors(CMemoryPool *mp,
											  CExpression *pexpr);

	// trim superfluos equality
	static CExpression *PexprPruneSuperfluousEquality(CMemoryPool *mp,
													  CExpression *pexpr);

	// trim existential subqueries
	static CExpression *PexprTrimExistentialSubqueries(CMemoryPool *mp,
													   CExpression *pexpr);

	// simplify quantified subqueries
	static CExpression *PexprSimplifyQuantifiedSubqueries(CMemoryPool *mp,
														  CExpression *pexpr);

	// preliminary unnesting of scalar  subqueries
	static CExpression *PexprUnnestScalarSubqueries(CMemoryPool *mp,
													CExpression *pexpr);

	// remove superfluous limit nodes
	static CExpression *PexprRemoveSuperfluousLimit(CMemoryPool *mp,
													CExpression *pexpr);

	// remove superfluous distinct nodes
	static CExpression *PexprRemoveSuperfluousDistinctInDQA(CMemoryPool *mp,
															CExpression *pexpr);

	// remove superfluous outer references from limit, group by and window operators
	static CExpression *PexprRemoveSuperfluousOuterRefs(CMemoryPool *mp,
														CExpression *pexpr);

	// generate predicates based on derived constraint properties
	static CExpression *PexprFromConstraints(
		CMemoryPool *mp, CExpression *pexpr, CColRefSet *pcrsProcessed,
		CPropConstraint *constraintsForOuterRefs);

	// generate predicates based on derived constraint properties under scalar expressions
	static CExpression *PexprFromConstraintsScalar(
		CMemoryPool *mp, CExpression *pexpr,
		CPropConstraint *constraintsForOuterRefs);

	// eliminate subtrees that have zero output cardinality
	static CExpression *PexprPruneEmptySubtrees(CMemoryPool *mp,
												CExpression *pexpr);

	// collapse cascaded inner joins into NAry-joins
	static CExpression *PexprCollapseJoins(CMemoryPool *mp, CExpression *pexpr);

	// helper method for PexprCollapseJoins, collect children and make recursive calls
	static void CollectJoinChildrenRecursively(
		CMemoryPool *mp, CExpression *pexpr, CExpressionArray *logicalLeafNodes,
		ULongPtrArray *lojChildPredIndexes,
		CExpressionArray *innerJoinPredicates, CExpressionArray *lojPredicates);

	// collapse cascaded logical project operators
	static CExpression *PexprCollapseProjects(CMemoryPool *mp,
											  CExpression *pexpr);

	// add dummy project element below scalar subquery when the output column is an outer reference
	static CExpression *PexprProjBelowSubquery(CMemoryPool *mp,
											   CExpression *pexpr,
											   BOOL fUnderPrList);

	// helper function to rewrite IN query to simple EXISTS with a predicate
	static CExpression *ConvertInToSimpleExists(CMemoryPool *mp,
												CExpression *pexpr);

	// rewrite IN subquery to EXIST subquery with a predicate
	static CExpression *PexprExistWithPredFromINSubq(CMemoryPool *mp,
													 CExpression *pexpr);

	// collapse cascaded union/union all into an NAry union/union all operator
	static CExpression *PexprCollapseUnionUnionAll(CMemoryPool *mp,
												   CExpression *pexpr);

	// transform outer joins into inner joins whenever possible
	static CExpression *PexprOuterJoinToInnerJoin(CMemoryPool *mp,
												  CExpression *pexpr);

	// eliminate CTE Anchors for CTEs that have zero consumers
	static CExpression *PexprRemoveUnusedCTEs(CMemoryPool *mp,
											  CExpression *pexpr);

	// collect CTE predicates from consumers
	static void CollectCTEPredicates(CMemoryPool *mp, CExpression *pexpr,
									 CTEPredsMap *phm);

	// imply new predicates on LOJ's inner child based on constraints derived from LOJ's outer child and join predicate
	static CExpression *PexprWithImpliedPredsOnLOJInnerChild(
		CMemoryPool *mp, CExpression *pexprLOJ, BOOL *pfAddedPredicates);

	// infer predicate from outer child to inner child of the outer join
	static CExpression *PexprOuterJoinInferPredsFromOuterChildToInnerChild(
		CMemoryPool *mp, CExpression *pexpr, BOOL *pfAddedPredicates);

	// driver for inferring predicates from constraints
	static CExpression *PexprInferPredicates(CMemoryPool *mp,
											 CExpression *pexpr);

	// entry for pruning unused computed columns
	static CExpression *PexprPruneUnusedComputedCols(CMemoryPool *mp,
													 CExpression *pexpr,
													 CColRefSet *pcrsReqd);

	// driver for pruning unused computed columns
	static CExpression *PexprPruneUnusedComputedColsRecursive(
		CMemoryPool *mp, CExpression *pexpr, CColRefSet *pcrsReqd);

	// prune unused project elements from the project list of Project or GbAgg
	static CExpression *PexprPruneProjListProjectOrGbAgg(
		CMemoryPool *mp, CExpression *pexpr, CColRefSet *pcrsUnused,
		CColRefSet *pcrsDefined, const CColRefSet *pcrsReqd);

	// generate a scalar bool op expression or return the only child expression in array
	static CExpression *PexprScalarBoolOpConvert2In(
		CMemoryPool *mp, CScalarBoolOp::EBoolOperator eboolop,
		CExpressionArray *pdrgpexpr);

	// determines if the expression is likely convertible to an array expression
	static BOOL FConvert2InIsConvertable(CExpression *pexpr,
										 CScalarBoolOp::EBoolOperator eboolop);

	// reorder the scalar cmp children to ensure that left child is Scalar Ident and right Child is Scalar Const
	static CExpression *PexprReorderScalarCmpChildren(CMemoryPool *mp,
													  CExpression *pexpr);

	// private ctor
	CExpressionPreprocessor();

	// private dtor
	virtual ~CExpressionPreprocessor();

	// private copy ctor
	CExpressionPreprocessor(const CExpressionPreprocessor &);

public:
	// main driver
	static CExpression *PexprPreprocess(
		CMemoryPool *mp, CExpression *pexpr,
		CColRefSet *pcrsOutputAndOrderCols = NULL);

	// add predicates collected from CTE consumers to producer expressions
	static void AddPredsToCTEProducers(CMemoryPool *mp, CExpression *pexpr);

	// derive constraints on given expression
	static CExpression *PexprAddPredicatesFromConstraints(CMemoryPool *mp,
														  CExpression *pexpr);

	// convert series of AND or OR comparisons into array IN expressions
	static CExpression *PexprConvert2In(CMemoryPool *mp, CExpression *pexpr);

};	// class CExpressionPreprocessor
}  // namespace gpopt


#endif	// !GPOPT_CExpressionPreprocessor_H

// EOF
