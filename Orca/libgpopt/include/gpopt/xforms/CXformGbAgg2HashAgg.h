//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CXformGbAgg2HashAgg.h
//
//	@doc:
//		Transform GbAgg to HashAgg
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformGbAgg2HashAgg_H
#define GPOPT_CXformGbAgg2HashAgg_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformGbAgg2HashAgg
//
//	@doc:
//		Transform Get to TableScan
//
//---------------------------------------------------------------------------
class CXformGbAgg2HashAgg : public CXformImplementation
{
private:
	// private copy ctor
	CXformGbAgg2HashAgg(const CXformGbAgg2HashAgg &);

protected:
	// check if the transformation is applicable
	BOOL FApplicable(CExpression *pexpr) const;

public:
	// ctor
	CXformGbAgg2HashAgg(CMemoryPool *mp);

	// ctor
	explicit CXformGbAgg2HashAgg(CExpression *pexprPattern);

	// dtor
	virtual ~CXformGbAgg2HashAgg()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfGbAgg2HashAgg;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformGbAgg2HashAgg";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformGbAgg2HashAgg

}  // namespace gpopt


#endif	// !GPOPT_CXformGbAgg2HashAgg_H

// EOF
