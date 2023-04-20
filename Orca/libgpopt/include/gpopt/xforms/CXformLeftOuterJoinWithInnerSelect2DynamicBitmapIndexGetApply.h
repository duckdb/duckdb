//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2019 Pivotal, Inc.
//
//	Transform Left Outer Join with a Select on the inner branch to
//	Bitmap IndexGet Apply for a partitioned table
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CXformLeftOuterJoinWithInnerSelect2DynamicBitmapIndexGetApply_H
#define GPOPT_CXformLeftOuterJoinWithInnerSelect2DynamicBitmapIndexGetApply_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformJoin2IndexApplyBase.h"

namespace gpopt
{
using namespace gpos;

class CXformLeftOuterJoinWithInnerSelect2DynamicBitmapIndexGetApply
	: public CXformJoin2IndexApplyBase<
		  CLogicalLeftOuterJoin, CLogicalIndexApply, CLogicalDynamicGet,
		  true /*fWithSelect*/, false /*is_partial*/, IMDIndex::EmdindBitmap>
{
private:
	// private copy ctor
	CXformLeftOuterJoinWithInnerSelect2DynamicBitmapIndexGetApply(
		const CXformLeftOuterJoinWithInnerSelect2DynamicBitmapIndexGetApply &);

public:
	// ctor
	explicit CXformLeftOuterJoinWithInnerSelect2DynamicBitmapIndexGetApply(
		CMemoryPool *mp)
		: CXformJoin2IndexApplyBase<CLogicalLeftOuterJoin, CLogicalIndexApply,
									CLogicalDynamicGet, true /*fWithSelect*/,
									false /*is_partial*/,
									IMDIndex::EmdindBitmap>(mp)
	{
	}

	// dtor
	virtual ~CXformLeftOuterJoinWithInnerSelect2DynamicBitmapIndexGetApply()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfLeftOuterJoinWithInnerSelect2DynamicBitmapIndexGetApply;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformLeftOuterJoinWithInnerSelect2DynamicBitmapIndexGetApply";
	}
};	// class CXformLeftOuterJoinWithInnerSelect2DynamicBitmapIndexGetApply
}  // namespace gpopt

#endif	// !GPOPT_CXformLeftOuterJoinWithInnerSelect2DynamicBitmapIndexGetApply_H

// EOF
