//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal, Inc.
//
//	@filename:
//		CXformInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply.h
//
//	@doc:
//		Transform Inner Join with a Select over a partitioned table on the inner
//		branch to a dynamic Bitmap IndexGet Apply
//
//	@owner:
//		n
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CXformInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply_H
#define GPOPT_CXformInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformJoin2IndexApplyBase.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply
//
//	@doc:
//		Transform Inner Join with a Select over a partitioned on the inner branch
//		to dynamic Bitmap IndexGet Apply
//
//---------------------------------------------------------------------------
class CXformInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply
	: public CXformJoin2IndexApplyBase<
		  CLogicalInnerJoin, CLogicalIndexApply, CLogicalDynamicGet,
		  true /*fWithSelect*/, false /*is_partial*/, IMDIndex::EmdindBitmap>
{
private:
	// private copy ctor
	CXformInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply(
		const CXformInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply &);

public:
	// ctor
	explicit CXformInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply(
		CMemoryPool *mp)
		: CXformJoin2IndexApplyBase<CLogicalInnerJoin, CLogicalIndexApply,
									CLogicalDynamicGet, true /*fWithSelect*/,
									false /*is_partial*/,
									IMDIndex::EmdindBitmap>(mp)
	{
	}

	// dtor
	virtual ~CXformInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply";
	}

	// return true if xform should be applied only once
	virtual BOOL
	IsApplyOnce()
	{
		return true;
	}

};	// class CXformInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply
}  // namespace gpopt

#endif	// !GPOPT_CXformInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply_H

// EOF
