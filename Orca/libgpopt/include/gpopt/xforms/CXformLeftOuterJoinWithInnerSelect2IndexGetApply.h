//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal, Inc.
//
//	Transform left outer Join with Select on the inner branch to IndexGet Apply
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CXformLeftOuterJoinWithInnerSelect2IndexGetApply_H
#define GPOPT_CXformLeftOuterJoinWithInnerSelect2IndexGetApply_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformJoin2IndexApplyBase.h"

namespace gpopt
{
using namespace gpos;

class CXformLeftOuterJoinWithInnerSelect2IndexGetApply
	: public CXformJoin2IndexApplyBase<
		  CLogicalLeftOuterJoin, CLogicalIndexApply, CLogicalGet,
		  true /*fWithSelect*/, false /*is_partial*/, IMDIndex::EmdindBtree>
{
private:
	// private copy ctor
	CXformLeftOuterJoinWithInnerSelect2IndexGetApply(
		const CXformLeftOuterJoinWithInnerSelect2IndexGetApply &);

public:
	// ctor
	explicit CXformLeftOuterJoinWithInnerSelect2IndexGetApply(CMemoryPool *mp)
		: CXformJoin2IndexApplyBase<CLogicalLeftOuterJoin, CLogicalIndexApply,
									CLogicalGet, true /*fWithSelect*/,
									false /*is_partial*/,
									IMDIndex::EmdindBtree>(mp)
	{
	}

	// dtor
	virtual ~CXformLeftOuterJoinWithInnerSelect2IndexGetApply()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfLeftOuterJoinWithInnerSelect2IndexGetApply;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformLeftOuterJoinWithInnerSelect2IndexGetApply";
	}
};
}  // namespace gpopt


#endif	// !GPOPT_CXformLeftOuterJoinWithInnerSelect2IndexGetApply_H

// EOF
