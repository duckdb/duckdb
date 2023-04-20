//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementUnionAll.h
//
//	@doc:
//		Transform Logical into Physical Union All
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementUnionAll_H
#define GPOPT_CXformImplementUnionAll_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformImplementUnionAll
//
//	@doc:
//		Transform Logical into Physical Union All
//
//---------------------------------------------------------------------------
class CXformImplementUnionAll : public CXformImplementation
{
private:
	// private copy ctor
	CXformImplementUnionAll(const CXformImplementUnionAll &);

public:
	// ctor
	explicit CXformImplementUnionAll(CMemoryPool *mp);

	// dtor
	virtual ~CXformImplementUnionAll()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfImplementUnionAll;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformImplementUnionAll";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise
	Exfp(CExpressionHandle &  // exprhdl
	) const
	{
		return CXform::ExfpHigh;
	}

	// actual transform
	void Transform(CXformContext *, CXformResult *, CExpression *) const;

};	// class CXformImplementUnionAll

}  // namespace gpopt

#endif	// !GPOPT_CXformImplementUnionAll_H

// EOF
