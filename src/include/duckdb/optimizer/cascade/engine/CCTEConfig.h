//---------------------------------------------------------------------------
//	@filename:
//		CCTEConfig.h
//
//	@doc:
//		CTE configurations
//---------------------------------------------------------------------------
#ifndef GPOPT_CCTEConfig_H
#define GPOPT_CCTEConfig_H

#include "duckdb/optimizer/cascade/base.h"

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
class CCTEConfig
{
public:
	// CTE inlining cut-off
	ULONG m_ulCTEInliningCutoff;

public:
	// ctor
	CCTEConfig(ULONG cte_inlining_cut_off)
		: m_ulCTEInliningCutoff(cte_inlining_cut_off)
	{
	}

	// private copy ctor
	CCTEConfig(const CCTEConfig &) = delete;

	// CTE inlining cut-off
	ULONG UlCTEInliningCutoff() const
	{
		return m_ulCTEInliningCutoff;
	}

	// generate default optimizer configurations
	static CCTEConfig* PcteconfDefault()
	{
		return new CCTEConfig(0);
	}
};	// class CCTEConfig
}  // namespace gpopt
#endif