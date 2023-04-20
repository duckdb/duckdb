//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformJoinAssociativity.h
//
//	@doc:
//		Transform left-deep join tree by associativity
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformJoinAssociativity_H
#define GPOPT_CXformJoinAssociativity_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformJoinAssociativity
//
//	@doc:
//		Associative transformation of left-deep join tree
//
//---------------------------------------------------------------------------
class CXformJoinAssociativity : public CXformExploration
{
private:
	// private copy ctor
	CXformJoinAssociativity(const CXformJoinAssociativity &);

	// helper function for creating the new join predicate
	void CreatePredicates(CMemoryPool *mp, CExpression *pexpr,
						  CExpressionArray *pdrgpexprLower,
						  CExpressionArray *pdrgpexprUpper) const;

public:
	// ctor
	explicit CXformJoinAssociativity(CMemoryPool *mp);

	// dtor
	virtual ~CXformJoinAssociativity()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfJoinAssociativity;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformJoinAssociativity";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformJoinAssociativity

}  // namespace gpopt


#endif	// !GPOPT_CXformJoinAssociativity_H

// EOF
