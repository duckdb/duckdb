//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2019 Pivotal, Inc.
//
//	Transform left outer Join to IndexGet Apply for a partitioned table
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftOuterJoin2DynamicIndexGetApply_H
#define GPOPT_CXformLeftOuterJoin2DynamicIndexGetApply_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformJoin2IndexApplyBase.h"

namespace gpopt
{
using namespace gpos;

class CXformLeftOuterJoin2DynamicIndexGetApply
	: public CXformJoin2IndexApplyBase<
		  CLogicalLeftOuterJoin, CLogicalIndexApply, CLogicalDynamicGet,
		  false /*fWithSelect*/, false /*is_partial*/, IMDIndex::EmdindBtree>
{
private:
	// private copy ctor
	CXformLeftOuterJoin2DynamicIndexGetApply(
		const CXformLeftOuterJoin2DynamicIndexGetApply &);

public:
	// ctor
	explicit CXformLeftOuterJoin2DynamicIndexGetApply(CMemoryPool *mp)
		: CXformJoin2IndexApplyBase<CLogicalLeftOuterJoin, CLogicalIndexApply,
									CLogicalDynamicGet, false /*fWithSelect*/,
									false /*is_partial*/,
									IMDIndex::EmdindBtree>(mp)
	{
	}

	// dtor
	virtual ~CXformLeftOuterJoin2DynamicIndexGetApply()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfLeftOuterJoin2DynamicIndexGetApply;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformLeftOuterJoin2DynamicIndexGetApply";
	}

};	// class CXformLeftOuterJoin2DynamicIndexGetApply

}  // namespace gpopt

#endif	// !GPOPT_CXformLeftOuterJoin2DynamicIndexGetApply_H

// EOF
