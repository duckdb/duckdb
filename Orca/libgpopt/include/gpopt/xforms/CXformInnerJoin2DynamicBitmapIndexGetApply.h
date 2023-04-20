//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CXformInnerJoin2DynamicBitmapIndexGetApply.h
//
//	@doc:
//		Transform Inner Join to Dynamic Bitmap IndexGet Apply
//
//	@owner:
//		n
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CXformInnerJoin2DynamicBitmapIndexGetApply_H
#define GPOPT_CXformInnerJoin2DynamicBitmapIndexGetApply_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformJoin2IndexApplyBase.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformInnerJoin2Dynamic BitmapIndexGetApply
//
//	@doc:
//		Transform Inner Join to Dynamic Bitmap IndexGet Apply
//
//---------------------------------------------------------------------------
class CXformInnerJoin2DynamicBitmapIndexGetApply
	: public CXformJoin2IndexApplyBase<
		  CLogicalInnerJoin, CLogicalIndexApply, CLogicalDynamicGet,
		  false /*fWithSelect*/, false /*is_partial*/, IMDIndex::EmdindBitmap>
{
private:
	// private copy ctor
	CXformInnerJoin2DynamicBitmapIndexGetApply(
		const CXformInnerJoin2DynamicBitmapIndexGetApply &);

public:
	// ctor
	explicit CXformInnerJoin2DynamicBitmapIndexGetApply(CMemoryPool *mp)
		: CXformJoin2IndexApplyBase<CLogicalInnerJoin, CLogicalIndexApply,
									CLogicalDynamicGet, false /*fWithSelect*/,
									false /*is_partial*/,
									IMDIndex::EmdindBitmap>(mp)
	{
	}

	// dtor
	virtual ~CXformInnerJoin2DynamicBitmapIndexGetApply()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfInnerJoin2DynamicBitmapIndexGetApply;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformInnerJoin2DynamicBitmapIndexGetApply";
	}

};	// class CXformInnerJoin2DynamicBitmapIndexGetApply
}  // namespace gpopt


#endif	// !GPOPT_CXformInnerJoin2DynamicBitmapIndexGetApply_H

// EOF
