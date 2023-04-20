//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformJoinCommutativity.h
//
//	@doc:
//		Transform join by commutativity
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformJoinCommutativity_H
#define GPOPT_CXformJoinCommutativity_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformJoinCommutativity
//
//	@doc:
//		Commutative transformation of join
//
//---------------------------------------------------------------------------
class CXformJoinCommutativity : public CXformExploration
{
private:
	// private copy ctor
	CXformJoinCommutativity(const CXformJoinCommutativity &);

public:
	// ctor
	explicit CXformJoinCommutativity(CMemoryPool *mp);

	// dtor
	virtual ~CXformJoinCommutativity()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfJoinCommutativity;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformJoinCommutativity";
	}

	// compatibility function
	BOOL FCompatible(CXform::EXformId exfid);

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

};	// class CXformJoinCommutativity

}  // namespace gpopt


#endif	// !GPOPT_CXformJoinCommutativity_H

// EOF
