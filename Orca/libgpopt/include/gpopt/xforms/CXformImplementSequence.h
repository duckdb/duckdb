//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementSequence.h
//
//	@doc:
//		Transform logical to physical Sequence
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementSequence_H
#define GPOPT_CXformImplementSequence_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformImplementSequence
//
//	@doc:
//		Transform logical to physical Sequence
//
//---------------------------------------------------------------------------
class CXformImplementSequence : public CXformImplementation
{
private:
	// private copy ctor
	CXformImplementSequence(const CXformImplementSequence &);

public:
	// ctor
	explicit CXformImplementSequence(CMemoryPool *);

	// dtor
	virtual ~CXformImplementSequence()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfImplementSequence;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformImplementSequence";
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

};	// class CXformImplementSequence

}  // namespace gpopt


#endif	// !GPOPT_CXformImplementSequence_H

// EOF
