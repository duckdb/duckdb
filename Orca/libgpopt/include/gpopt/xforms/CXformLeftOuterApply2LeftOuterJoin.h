//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformLeftOuterApply2LeftOuterJoin.h
//
//	@doc:
//		Turn left outer apply into left outer join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftOuterApply2LeftOuterJoin_H
#define GPOPT_CXformLeftOuterApply2LeftOuterJoin_H

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformApply2Join.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformLeftOuterApply2LeftOuterJoin
//
//	@doc:
//		Transform Apply into Join by decorrelating the inner side
//
//---------------------------------------------------------------------------
class CXformLeftOuterApply2LeftOuterJoin
	: public CXformApply2Join<CLogicalLeftOuterApply, CLogicalLeftOuterJoin>
{
private:
	// private copy ctor
	CXformLeftOuterApply2LeftOuterJoin(
		const CXformLeftOuterApply2LeftOuterJoin &);

public:
	// ctor
	explicit CXformLeftOuterApply2LeftOuterJoin(CMemoryPool *mp)
		: CXformApply2Join<CLogicalLeftOuterApply, CLogicalLeftOuterJoin>(
			  mp, true /*fDeepTree*/)
	{
	}

	// dtor
	virtual ~CXformLeftOuterApply2LeftOuterJoin()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfLeftOuterApply2LeftOuterJoin;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformLeftOuterApply2LeftOuterJoin";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;


};	// class CXformLeftOuterApply2LeftOuterJoin

}  // namespace gpopt

#endif	// !GPOPT_CXformLeftOuterApply2LeftOuterJoin_H

// EOF
