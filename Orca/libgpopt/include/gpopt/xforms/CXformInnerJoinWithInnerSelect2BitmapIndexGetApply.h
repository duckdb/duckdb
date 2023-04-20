//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal, Inc.
//
//	@filename:
//		CXformInnerJoinWithInnerSelect2BitmapIndexGetApply.h
//
//	@doc:
//		Transform Inner Join with a Select on the inner branch to
//		Bitmap IndexGet Apply
//
//	@owner:
//		n
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CXformInnerJoinWithInnerSelect2BitmapIndexGetApply_H
#define GPOPT_CXformInnerJoinWithInnerSelect2BitmapIndexGetApply_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformJoin2IndexApplyBase.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformInnerJoinWithInnerSelect2BitmapIndexGetApply
//
//	@doc:
//		Transform Inner Join with a Select on the inner branch to
//		Bitmap IndexGet Apply
//
//---------------------------------------------------------------------------
class CXformInnerJoinWithInnerSelect2BitmapIndexGetApply
	: public CXformJoin2IndexApplyBase<
		  CLogicalInnerJoin, CLogicalIndexApply, CLogicalGet,
		  true /*fWithSelect*/, false /*is_partial*/, IMDIndex::EmdindBitmap>
{
private:
	// private copy ctor
	CXformInnerJoinWithInnerSelect2BitmapIndexGetApply(
		const CXformInnerJoinWithInnerSelect2BitmapIndexGetApply &);

public:
	// ctor
	explicit CXformInnerJoinWithInnerSelect2BitmapIndexGetApply(CMemoryPool *mp)
		: CXformJoin2IndexApplyBase<CLogicalInnerJoin, CLogicalIndexApply,
									CLogicalGet, true /*fWithSelect*/,
									false /*is_partial*/,
									IMDIndex::EmdindBitmap>(mp)
	{
	}

	// dtor
	virtual ~CXformInnerJoinWithInnerSelect2BitmapIndexGetApply()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfInnerJoinWithInnerSelect2BitmapIndexGetApply;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformInnerJoinWithInnerSelect2BitmapIndexGetApply";
	}

};	// class CXformInnerJoinWithInnerSelect2BitmapIndexGetApply
}  // namespace gpopt

#endif	// !GPOPT_CXformInnerJoinWithInnerSelect2BitmapIndexGetApply_H

// EOF
