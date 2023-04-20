//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp
//
//	@filename:
//		CXformImplementDML.h
//
//	@doc:
//		Transform Logical DML to Physical DML
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementDML_H
#define GPOPT_CXformImplementDML_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformImplementDML
//
//	@doc:
//		Transform Logical DML to Physical DML
//
//---------------------------------------------------------------------------
class CXformImplementDML : public CXformImplementation
{
private:
	// private copy ctor
	CXformImplementDML(const CXformImplementDML &);

public:
	// ctor
	explicit CXformImplementDML(CMemoryPool *mp);

	// dtor
	virtual ~CXformImplementDML()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfImplementDML;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformImplementDML";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformImplementDML
}  // namespace gpopt

#endif	// !GPOPT_CXformImplementDML_H

// EOF
