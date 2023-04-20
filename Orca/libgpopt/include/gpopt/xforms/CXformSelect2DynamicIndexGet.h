//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSelect2DynamicIndexGet.h
//
//	@doc:
//		Transform select over partitioned table into a dynamic index get
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformSelect2DynamicIndexGet_H
#define GPOPT_CXformSelect2DynamicIndexGet_H

#include "gpos/base.h"

#include "gpopt/operators/CLogicalGet.h"
#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformSelect2DynamicIndexGet
//
//	@doc:
//		Transform select over a partitioned table into a dynamic index get
//
//---------------------------------------------------------------------------
class CXformSelect2DynamicIndexGet : public CXformExploration
{
private:
	// private copy ctor
	CXformSelect2DynamicIndexGet(const CXformSelect2DynamicIndexGet &);

	// return the column reference set of included / key columns
	CColRefSet *GetColRefSet(CMemoryPool *mp, CLogicalGet *popGet,
							 const IMDIndex *pmdindex,
							 BOOL fIncludedColumns) const;

public:
	// ctor
	explicit CXformSelect2DynamicIndexGet(CMemoryPool *mp);

	// dtor
	virtual ~CXformSelect2DynamicIndexGet()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfSelect2DynamicIndexGet;
	}

	// xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformSelect2DynamicIndexGet";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;


};	// class CXformSelect2DynamicIndexGet

}  // namespace gpopt

#endif	// !GPOPT_CXformSelect2DynamicIndexGet_H

// EOF
