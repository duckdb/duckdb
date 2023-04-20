//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal Inc.
//
//	@filename:
//		CXformSplitDQA.h
//
//	@doc:
//		Split an aggregate into a three level aggregation
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformSplitDQA_H
#define GPOPT_CXformSplitDQA_H

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CLogicalGbAgg.h"
#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformSplitDQA
//
//	@doc:
//		Split an aggregate into a three level aggregation
//
//---------------------------------------------------------------------------
class CXformSplitDQA : public CXformExploration
{
private:
	// hash map between expression and a column reference
	typedef CHashMap<CExpression, CColRef, CExpression::HashValue,
					 CUtils::Equals, CleanupRelease<CExpression>,
					 CleanupNULL<CColRef> >
		ExprToColRefMap;

	// private copy ctor
	CXformSplitDQA(const CXformSplitDQA &);

	// generate an expression with multi-level aggregation
	static CExpression *PexprMultiLevelAggregation(
		CMemoryPool *mp, CExpression *pexprRelational,
		CExpressionArray *pdrgpexprPrElFirstStage,
		CExpressionArray *pdrgpexprPrElSecondStage,
		CExpressionArray *pdrgpexprPrElThirdStage, CColRefArray *pdrgpcrArgDQA,
		CColRefArray *pdrgpcrLastStage, BOOL fSplit2LevelsOnly,
		BOOL fAddDistinctColToLocalGb, CLogicalGbAgg::EAggStage aggStage);

	// split DQA into a local DQA and global non-DQA aggregate function
	static CExpression *PexprSplitIntoLocalDQAGlobalAgg(
		CMemoryPool *mp, CColumnFactory *col_factory, CMDAccessor *md_accessor,
		CExpression *pexpr, CExpression *pexprRelational,
		ExprToColRefMap *phmexprcr, CColRefArray *pdrgpcrArgDQA,
		CLogicalGbAgg::EAggStage aggStage);

	// helper function to split DQA
	static CExpression *PexprSplitHelper(
		CMemoryPool *mp, CColumnFactory *col_factory, CMDAccessor *md_accessor,
		CExpression *pexpr, CExpression *pexprRelational,
		ExprToColRefMap *phmexprcr, CColRefArray *pdrgpcrArgDQA,
		CLogicalGbAgg::EAggStage aggStage);

	// given a scalar aggregate generate the local, intermediate and global
	// aggregate functions. Then add them to the project list of the
	// corresponding aggregate operator at each stage of the multi-stage
	// aggregation
	static void PopulatePrLMultiPhaseAgg(
		CMemoryPool *mp, CColumnFactory *col_factory, CMDAccessor *md_accessor,
		CExpression *pexprPrEl, CExpressionArray *pdrgpexprPrElFirstStage,
		CExpressionArray *pdrgpexprPrElSecondStage,
		CExpressionArray *pdrgpexprPrElLastStage, BOOL fSplit2LevelsOnly);

	// create project element for the aggregate function of a particular level
	static CExpression *PexprPrElAgg(CMemoryPool *mp, CExpression *pexprAggFunc,
									 EAggfuncStage eaggfuncstage,
									 CColRef *pcrPreviousStage,
									 CColRef *pcrGlobal);

	// extract arguments of distinct aggs
	static void ExtractDistinctCols(
		CMemoryPool *mp, CColumnFactory *col_factory, CMDAccessor *md_accessor,
		CExpression *pexpr, CExpressionArray *pdrgpexprChildPrEl,
		ExprToColRefMap *phmexprcr, CColRefArray **ppdrgpcrArgDQA);

	// return the column reference of the argument to the aggregate function
	static CColRef *PcrAggFuncArgument(CMemoryPool *mp,
									   CMDAccessor *md_accessor,
									   CColumnFactory *col_factory,
									   CExpression *pexprArg,
									   CExpressionArray *pdrgpexprChildPrEl);

public:
	// ctor
	explicit CXformSplitDQA(CMemoryPool *mp);

	// dtor
	virtual ~CXformSplitDQA()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfSplitDQA;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformSplitDQA";
	}

	// Compatibility function for splitting aggregates
	virtual BOOL
	FCompatible(CXform::EXformId exfid)
	{
		return (CXform::ExfSplitDQA != exfid) &&
			   (CXform::ExfSplitGbAgg != exfid);
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *, CXformResult *, CExpression *) const;

};	// class CXformSplitDQA

}  // namespace gpopt

#endif	// !GPOPT_CXformSplitDQA_H

// EOF
