//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformLeftOuterApply2LeftOuterJoinNoCorrelations.h
//
//	@doc:
//		Turn inner Apply into Inner Join when Apply's inner child has no
//		correlations
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftOuterApply2LeftOuterJoinNoCorrelations_H
#define GPOPT_CXformLeftOuterApply2LeftOuterJoinNoCorrelations_H

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformApply2Join.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformLeftOuterApply2LeftOuterJoinNoCorrelations
//
//	@doc:
//		Transform left outer apply into left outer join
//
//---------------------------------------------------------------------------
class CXformLeftOuterApply2LeftOuterJoinNoCorrelations
	: public CXformApply2Join<CLogicalLeftOuterApply, CLogicalLeftOuterJoin>
{
private:
	// private copy ctor
	CXformLeftOuterApply2LeftOuterJoinNoCorrelations(
		const CXformLeftOuterApply2LeftOuterJoinNoCorrelations &);

public:
	// ctor
	explicit CXformLeftOuterApply2LeftOuterJoinNoCorrelations(CMemoryPool *mp)
		: CXformApply2Join<CLogicalLeftOuterApply, CLogicalLeftOuterJoin>(mp)
	{
	}

	// dtor
	virtual ~CXformLeftOuterApply2LeftOuterJoinNoCorrelations()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfLeftOuterApply2LeftOuterJoinNoCorrelations;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformLeftOuterApply2LeftOuterJoinNoCorrelations";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformLeftOuterApply2LeftOuterJoinNoCorrelations

}  // namespace gpopt

#endif	// !GPOPT_CXformLeftOuterApply2LeftOuterJoinNoCorrelations_H

// EOF
