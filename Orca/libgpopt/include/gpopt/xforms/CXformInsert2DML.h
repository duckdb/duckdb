//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp
//
//	@filename:
//		CXformInsert2DML.h
//
//	@doc:
//		Transform Logical Insert to Logical DML
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformInsert2DML_H
#define GPOPT_CXformInsert2DML_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformInsert2DML
//
//	@doc:
//		Transform Logical Insert to Logical DML
//
//---------------------------------------------------------------------------
class CXformInsert2DML : public CXformExploration
{
private:
	// private copy ctor
	CXformInsert2DML(const CXformInsert2DML &);

public:
	// ctor
	explicit CXformInsert2DML(CMemoryPool *mp);

	// dtor
	virtual ~CXformInsert2DML()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfInsert2DML;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformInsert2DML";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	virtual void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
						   CExpression *pexpr) const;

};	// class CXformInsert2DML
}  // namespace gpopt

#endif	// !GPOPT_CXformInsert2DML_H

// EOF
