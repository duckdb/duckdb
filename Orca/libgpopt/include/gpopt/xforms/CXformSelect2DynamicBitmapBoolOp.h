//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CXformSelect2DynamicBitmapBoolOp.h
//
//	@doc:
//		Transform select over partitioned table into a dynamic bitmap table get
//		with a bitmap bool op child
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformSelect2DynamicBitmapBoolOp_H
#define GPOPT_CXformSelect2DynamicBitmapBoolOp_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CXformSelect2DynamicBitmapBoolOp
//
//	@doc:
//		Transform select over partitioned table table into a dynamic bitmap
//		table get with bitmap bool op
//---------------------------------------------------------------------------
class CXformSelect2DynamicBitmapBoolOp : public CXformExploration
{
private:
	// disable copy ctor
	CXformSelect2DynamicBitmapBoolOp(const CXformSelect2DynamicBitmapBoolOp &);

public:
	// ctor
	explicit CXformSelect2DynamicBitmapBoolOp(CMemoryPool *mp);

	// dtor
	virtual ~CXformSelect2DynamicBitmapBoolOp()
	{
	}

	// identifier
	virtual EXformId
	Exfid() const
	{
		return ExfSelect2DynamicBitmapBoolOp;
	}

	// xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformSelect2DynamicBitmapBoolOp";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	virtual void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
						   CExpression *pexpr) const;

};	// class CXformSelect2DynamicBitmapBoolOp
}  // namespace gpopt

#endif	// !GPOPT_CXformSelect2DynamicBitmapBoolOp_H

// EOF
