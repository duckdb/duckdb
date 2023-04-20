//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformUnnestTVF.h
//
//	@doc:
//		 Unnest TVF with subquery arguments
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformUnnestTVF_H
#define GPOPT_CXformUnnestTVF_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformUnnestTVF
//
//	@doc:
//		Unnest TVF with subquery arguments
//
//---------------------------------------------------------------------------
class CXformUnnestTVF : public CXformExploration
{
private:
	// private copy ctor
	CXformUnnestTVF(const CXformUnnestTVF &);

	// helper for mapping subquery function arguments into columns
	static CColRefArray *PdrgpcrSubqueries(CMemoryPool *mp,
										   CExpression *pexprCTEProducer,
										   CExpression *pexprCTEConsumer);

	//	collect subquery arguments and return a Project expression
	static CExpression *PexprProjectSubqueries(CMemoryPool *mp,
											   CExpression *pexprTVF);

public:
	// ctor
	explicit CXformUnnestTVF(CMemoryPool *mp);

	// dtor
	virtual ~CXformUnnestTVF()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfUnnestTVF;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformUnnestTVF";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	virtual void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
						   CExpression *pexpr) const;

};	// class CXformUnnestTVF

}  // namespace gpopt

#endif	// !GPOPT_CXformUnnestTVF_H

// EOF
