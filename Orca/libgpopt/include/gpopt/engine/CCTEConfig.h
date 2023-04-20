//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CCTEConfig.h
//
//	@doc:
//		CTE configurations
//---------------------------------------------------------------------------
#ifndef GPOPT_CCTEConfig_H
#define GPOPT_CCTEConfig_H

#include "gpos/base.h"
#include "gpos/common/CDouble.h"
#include "gpos/common/CRefCount.h"
#include "gpos/memory/CMemoryPool.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CCTEConfig
//
//	@doc:
//		CTE configurations
//
//---------------------------------------------------------------------------
class CCTEConfig : public CRefCount
{
private:
	// CTE inlining cut-off
	ULONG m_ulCTEInliningCutoff;

	// private copy ctor
	CCTEConfig(const CCTEConfig &);

public:
	// ctor
	CCTEConfig(ULONG cte_inlining_cut_off)
		: m_ulCTEInliningCutoff(cte_inlining_cut_off)
	{
	}

	// CTE inlining cut-off
	ULONG
	UlCTEInliningCutoff() const
	{
		return m_ulCTEInliningCutoff;
	}

	// generate default optimizer configurations
	static CCTEConfig *
	PcteconfDefault(CMemoryPool *mp)
	{
		return GPOS_NEW(mp) CCTEConfig(0 /* cte_inlining_cut_off */);
	}

};	// class CCTEConfig
}  // namespace gpopt

#endif	// !GPOPT_CCTEConfig_H

// EOF
