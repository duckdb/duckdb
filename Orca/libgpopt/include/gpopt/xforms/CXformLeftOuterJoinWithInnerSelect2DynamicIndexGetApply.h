//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2019 Pivotal, Inc.
//
//	Transform left outer Join with Select on the inner branch to IndexGet Apply
//	for a partitioned table
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CXformLeftOuterJoinWithInnerSelect2DynamicIndexGetApply_H
#define GPOPT_CXformLeftOuterJoinWithInnerSelect2DynamicIndexGetApply_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformJoin2IndexApplyBase.h"

namespace gpopt
{
using namespace gpos;

class CXformLeftOuterJoinWithInnerSelect2DynamicIndexGetApply
	: public CXformJoin2IndexApplyBase<
		  CLogicalLeftOuterJoin, CLogicalIndexApply, CLogicalDynamicGet,
		  true /*fWithSelect*/, false /*is_partial*/, IMDIndex::EmdindBtree>
{
private:
	// private copy ctor
	CXformLeftOuterJoinWithInnerSelect2DynamicIndexGetApply(
		const CXformLeftOuterJoinWithInnerSelect2DynamicIndexGetApply &);

public:
	// ctor
	explicit CXformLeftOuterJoinWithInnerSelect2DynamicIndexGetApply(
		CMemoryPool *mp)
		: CXformJoin2IndexApplyBase<CLogicalLeftOuterJoin, CLogicalIndexApply,
									CLogicalDynamicGet, true /*fWithSelect*/,
									false /*is_partial*/,
									IMDIndex::EmdindBtree>(mp)
	{
	}

	// dtor
	virtual ~CXformLeftOuterJoinWithInnerSelect2DynamicIndexGetApply()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfLeftOuterJoinWithInnerSelect2DynamicIndexGetApply;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformLeftOuterJoinWithInnerSelect2DynamicIndexGetApply";
	}
};
}  // namespace gpopt


#endif	// !GPOPT_CXformLeftOuterJoinWithInnerSelect2DynamicIndexGetApply_H

// EOF
