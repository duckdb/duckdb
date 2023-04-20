//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformGbAgg2StreamAgg.h
//
//	@doc:
//		Transform GbAgg to StreamAgg
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformGbAgg2StreamAgg_H
#define GPOPT_CXformGbAgg2StreamAgg_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformGbAgg2StreamAgg
//
//	@doc:
//		Transform GbAgg to Stream Agg
//
//---------------------------------------------------------------------------
class CXformGbAgg2StreamAgg : public CXformImplementation
{
private:
	// private copy ctor
	CXformGbAgg2StreamAgg(const CXformGbAgg2StreamAgg &);

public:
	// ctor
	CXformGbAgg2StreamAgg(CMemoryPool *mp);

	// ctor
	explicit CXformGbAgg2StreamAgg(CExpression *pexprPattern);

	// dtor
	virtual ~CXformGbAgg2StreamAgg()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfGbAgg2StreamAgg;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformGbAgg2StreamAgg";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformGbAgg2StreamAgg

}  // namespace gpopt


#endif	// !GPOPT_CXformGbAgg2StreamAgg_H

// EOF
