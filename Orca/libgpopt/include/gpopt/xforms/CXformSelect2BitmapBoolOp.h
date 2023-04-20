//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CXformSelect2BitmapBoolOp.h
//
//	@doc:
//		Transform select over table into a bitmap table get with bitmap bool op
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformSelect2BitmapBoolOp_H
#define GPOPT_CXformSelect2BitmapBoolOp_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CXformSelect2BitmapBoolOp
//
//	@doc:
//		Transform select over a table into a bitmap table get with bitmap bool op
//
//---------------------------------------------------------------------------
class CXformSelect2BitmapBoolOp : public CXformExploration
{
private:
	// disable copy ctor
	CXformSelect2BitmapBoolOp(const CXformSelect2BitmapBoolOp &);

public:
	// ctor
	explicit CXformSelect2BitmapBoolOp(CMemoryPool *mp);

	// dtor
	virtual ~CXformSelect2BitmapBoolOp()
	{
	}

	// identifier
	virtual EXformId
	Exfid() const
	{
		return ExfSelect2BitmapBoolOp;
	}

	// xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformSelect2BitmapBoolOp";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	virtual void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
						   CExpression *pexpr) const;

};	// class CXformSelect2BitmapBoolOp
}  // namespace gpopt

#endif	// !GPOPT_CXformSelect2BitmapBoolOp_H

// EOF
