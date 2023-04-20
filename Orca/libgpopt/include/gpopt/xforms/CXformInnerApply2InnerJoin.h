//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformInnerApply2InnerJoin.h
//
//	@doc:
//		Turn inner apply into inner join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformInnerApply2InnerJoin_H
#define GPOPT_CXformInnerApply2InnerJoin_H

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformApply2Join.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformInnerApply2InnerJoin
//
//	@doc:
//		Transform Apply into Join by decorrelating the inner side
//
//---------------------------------------------------------------------------
class CXformInnerApply2InnerJoin
	: public CXformApply2Join<CLogicalInnerApply, CLogicalInnerJoin>
{
private:
	// private copy ctor
	CXformInnerApply2InnerJoin(const CXformInnerApply2InnerJoin &);

public:
	// ctor
	explicit CXformInnerApply2InnerJoin(CMemoryPool *mp)
		: CXformApply2Join<CLogicalInnerApply, CLogicalInnerJoin>(
			  mp, true /*fDeepTree*/)
	{
	}

	// dtor
	virtual ~CXformInnerApply2InnerJoin()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfInnerApply2InnerJoin;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformInnerApply2InnerJoin";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformInnerApply2InnerJoin

}  // namespace gpopt

#endif	// !GPOPT_CXformInnerApply2InnerJoin_H

// EOF
