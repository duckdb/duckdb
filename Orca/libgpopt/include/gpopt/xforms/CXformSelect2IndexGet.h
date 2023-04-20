//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSelect2IndexGet.h
//
//	@doc:
//		Transform select over table into an index get
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformSelect2IndexGet_H
#define GPOPT_CXformSelect2IndexGet_H

#include "gpos/base.h"

#include "gpopt/operators/CLogicalGet.h"
#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformSelect2IndexGet
//
//	@doc:
//		Transform select over a table into an index get
//
//---------------------------------------------------------------------------
class CXformSelect2IndexGet : public CXformExploration
{
private:
	// private copy ctor
	CXformSelect2IndexGet(const CXformSelect2IndexGet &);

	// return the column reference set of included / key columns
	CColRefSet *GetColRefSet(CMemoryPool *mp, CLogicalGet *popGet,
							 const IMDIndex *pmdindex,
							 BOOL fIncludedColumns) const;

public:
	// ctor
	explicit CXformSelect2IndexGet(CMemoryPool *mp);

	// dtor
	virtual ~CXformSelect2IndexGet()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfSelect2IndexGet;
	}

	// xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformSelect2IndexGet";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;


};	// class CXformSelect2IndexGet

}  // namespace gpopt

#endif	// !GPOPT_CXformSelect2IndexGet_H

// EOF
