//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2020 VMware, Inc.
//
//	@filename:
//		CXformIndexGet2IndexOnlyScan.h
//
//	@doc:
//		Transform Index Get to Index Only Scan
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformIndexGet2IndexOnlyScan_H
#define GPOPT_CXformIndexGet2IndexOnlyScan_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformIndexGet2IndexOnlyScan
//
//	@doc:
//		Transform Index Get to Index Scan
//
//---------------------------------------------------------------------------
class CXformIndexGet2IndexOnlyScan : public CXformImplementation
{
private:
public:
	CXformIndexGet2IndexOnlyScan(const CXformIndexGet2IndexOnlyScan &) = delete;

	// ctor
	explicit CXformIndexGet2IndexOnlyScan(CMemoryPool *);

	// dtor
	~CXformIndexGet2IndexOnlyScan() override = default;

	// ident accessors
	EXformId
	Exfid() const override
	{
		return ExfIndexGet2IndexOnlyScan;
	}

	// xform name
	const CHAR *
	SzId() const override
	{
		return "CXformIndexGet2IndexOnlyScan";
	}

	// compute xform promise for a given expression handle
	EXformPromise Exfp(CExpressionHandle &	//exprhdl
	) const override;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const override;

};	// class CXformIndexGet2IndexOnlyScan

}  // namespace gpopt

#endif	// !GPOPT_CXformIndexGet2IndexOnlyScan_H

// EOF
