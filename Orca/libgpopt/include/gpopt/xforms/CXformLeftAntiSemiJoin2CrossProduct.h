//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformLeftAntiSemiJoin2CrossProduct.h
//
//	@doc:
//		Transform left anti semi join to cross product
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftAntiSemiJoin2CrossProduct_H
#define GPOPT_CXformLeftAntiSemiJoin2CrossProduct_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformLeftAntiSemiJoin2CrossProduct
//
//	@doc:
//		Transform left anti semi join to cross product
//
//---------------------------------------------------------------------------
class CXformLeftAntiSemiJoin2CrossProduct : public CXformExploration
{
private:
	// private copy ctor
	CXformLeftAntiSemiJoin2CrossProduct(
		const CXformLeftAntiSemiJoin2CrossProduct &);

public:
	// ctor
	explicit CXformLeftAntiSemiJoin2CrossProduct(CMemoryPool *mp);

	// ctor
	explicit CXformLeftAntiSemiJoin2CrossProduct(CExpression *pexprPattern);

	// dtor
	virtual ~CXformLeftAntiSemiJoin2CrossProduct()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfLeftAntiSemiJoin2CrossProduct;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformLeftAntiSemiJoin2CrossProduct";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformLeftAntiSemiJoin2CrossProduct

}  // namespace gpopt


#endif	// !GPOPT_CXformLeftAntiSemiJoin2CrossProduct_H

// EOF
