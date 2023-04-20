//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CXformInnerJoinWithInnerSelect2IndexGetApply.h
//
//	@doc:
//		Transform Inner Join with Select on the inner branch to IndexGet Apply
//
//	@owner:
//		n
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CXformInnerJoinWithInnerSelect2IndexGetApply_H
#define GPOPT_CXformInnerJoinWithInnerSelect2IndexGetApply_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformJoin2IndexApplyBase.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformInnerJoinWithInnerSelect2IndexGetApply
//
//	@doc:
//		Transform Inner Join with Select on the inner branch to IndexGet Apply
//
//---------------------------------------------------------------------------
class CXformInnerJoinWithInnerSelect2IndexGetApply
	: public CXformJoin2IndexApplyBase<
		  CLogicalInnerJoin, CLogicalIndexApply, CLogicalGet,
		  true /*fWithSelect*/, false /*is_partial*/, IMDIndex::EmdindBtree>
{
private:
	// private copy ctor
	CXformInnerJoinWithInnerSelect2IndexGetApply(
		const CXformInnerJoinWithInnerSelect2IndexGetApply &);

public:
	// ctor
	explicit CXformInnerJoinWithInnerSelect2IndexGetApply(CMemoryPool *mp)
		: CXformJoin2IndexApplyBase<CLogicalInnerJoin, CLogicalIndexApply,
									CLogicalGet, true /*fWithSelect*/,
									false /*is_partial*/,
									IMDIndex::EmdindBtree>(mp)
	{
	}

	// dtor
	virtual ~CXformInnerJoinWithInnerSelect2IndexGetApply()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfInnerJoinWithInnerSelect2IndexGetApply;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformInnerJoinWithInnerSelect2IndexGetApply";
	}
};
}  // namespace gpopt


#endif	// !GPOPT_CXformInnerJoinWithInnerSelect2IndexGetApply_H

// EOF
