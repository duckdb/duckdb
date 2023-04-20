//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal Inc.
//
//	@filename:
//		CXformIntersect2Join.h
//
//	@doc:
//		Class to transform of Intersect into a Join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformIntersect2Join_H
#define GPOPT_CXformIntersect2Join_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformIntersect2Join
//
//	@doc:
//		Class to transform of Intersect into a Join
//
//---------------------------------------------------------------------------
class CXformIntersect2Join : public CXformExploration
{
private:
	// private copy ctor
	CXformIntersect2Join(const CXformIntersect2Join &);

public:
	// ctor
	explicit CXformIntersect2Join(CMemoryPool *mp);

	// dtor
	virtual ~CXformIntersect2Join()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfIntersect2Join;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformIntersect2Join";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise
	Exfp(CExpressionHandle &  // exprhdl
	) const
	{
		return CXform::ExfpHigh;
	}

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformIntersect2Join

}  // namespace gpopt

#endif	// !GPOPT_CXformIntersect2Join_H

// EOF
