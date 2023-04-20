//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal, Inc.
//
//	Transform Left Outer Join with a Select on the inner branch to
//	Bitmap IndexGet Apply
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CXformLeftOuterJoinWithInnerSelect2BitmapIndexGetApply_H
#define GPOPT_CXformLeftOuterJoinWithInnerSelect2BitmapIndexGetApply_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformJoin2IndexApplyBase.h"

namespace gpopt
{
using namespace gpos;

class CXformLeftOuterJoinWithInnerSelect2BitmapIndexGetApply
	: public CXformJoin2IndexApplyBase<
		  CLogicalLeftOuterJoin, CLogicalIndexApply, CLogicalGet,
		  true /*fWithSelect*/, false /*is_partial*/, IMDIndex::EmdindBitmap>
{
private:
	// private copy ctor
	CXformLeftOuterJoinWithInnerSelect2BitmapIndexGetApply(
		const CXformLeftOuterJoinWithInnerSelect2BitmapIndexGetApply &);

public:
	// ctor
	explicit CXformLeftOuterJoinWithInnerSelect2BitmapIndexGetApply(
		CMemoryPool *mp)
		: CXformJoin2IndexApplyBase<CLogicalLeftOuterJoin, CLogicalIndexApply,
									CLogicalGet, true /*fWithSelect*/,
									false /*is_partial*/,
									IMDIndex::EmdindBitmap>(mp)
	{
	}

	// dtor
	virtual ~CXformLeftOuterJoinWithInnerSelect2BitmapIndexGetApply()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfLeftOuterJoinWithInnerSelect2BitmapIndexGetApply;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformLeftOuterJoinWithInnerSelect2BitmapIndexGetApply";
	}
};	// class CXformLeftOuterJoinWithInnerSelect2BitmapIndexGetApply
}  // namespace gpopt

#endif	// !GPOPT_CXformLeftOuterJoinWithInnerSelect2BitmapIndexGetApply_H

// EOF
