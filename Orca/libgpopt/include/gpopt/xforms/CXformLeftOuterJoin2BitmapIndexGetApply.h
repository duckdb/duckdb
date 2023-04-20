//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal, Inc.
//
//	Transform Left Outer Join to Bitmap IndexGet Apply
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CXformLeftOuterJoin2BitmapIndexGetApply_H
#define GPOPT_CXformLeftOuterJoin2BitmapIndexGetApply_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformJoin2IndexApplyBase.h"

namespace gpopt
{
using namespace gpos;

class CXformLeftOuterJoin2BitmapIndexGetApply
	: public CXformJoin2IndexApplyBase<
		  CLogicalLeftOuterJoin, CLogicalIndexApply, CLogicalGet,
		  false /*fWithSelect*/, false /*is_partial*/, IMDIndex::EmdindBitmap>
{
private:
	// private copy ctor
	CXformLeftOuterJoin2BitmapIndexGetApply(
		const CXformLeftOuterJoin2BitmapIndexGetApply &);

public:
	// ctor
	explicit CXformLeftOuterJoin2BitmapIndexGetApply(CMemoryPool *mp)
		: CXformJoin2IndexApplyBase<CLogicalLeftOuterJoin, CLogicalIndexApply,
									CLogicalGet, false /*fWithSelect*/,
									false /*is_partial*/,
									IMDIndex::EmdindBitmap>(mp)
	{
	}

	// dtor
	virtual ~CXformLeftOuterJoin2BitmapIndexGetApply()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfLeftOuterJoin2BitmapIndexGetApply;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformLeftOuterJoin2BitmapIndexGetApply";
	}
};	// class CXformLeftOuterJoin2BitmapIndexGetApply
}  // namespace gpopt

#endif	// !GPOPT_CXformLeftOuterJoin2BitmapIndexGetApply_H

// EOF
