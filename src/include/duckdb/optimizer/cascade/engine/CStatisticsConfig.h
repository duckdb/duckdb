//---------------------------------------------------------------------------
//	@filename:
//		CStatisticsConfig.h
//
//	@doc:
//		Statistics configurations
//---------------------------------------------------------------------------
#ifndef GPOPT_CStatisticsConfig_H
#define GPOPT_CStatisticsConfig_H

#include "duckdb/optimizer/cascade/base.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CStatisticsConfig
//
//	@doc:
//		Statistics configurations
//
//---------------------------------------------------------------------------
class CStatisticsConfig
{
public:
	// damping factor for filter
	double m_damping_factor_filter;

	// damping factor for join
	double m_damping_factor_join;

	// damping factor for group by
	double m_damping_factor_groupby;

public:
	// ctor
	CStatisticsConfig(double damping_factor_filter, double damping_factor_join, double damping_factor_groupby);

	// dtor
	~CStatisticsConfig();

	// damping factor for filter
	double DDampingFactorFilter() const
	{
		return m_damping_factor_filter;
	}

	// damping factor for join
	double DDampingFactorJoin() const
	{
		return m_damping_factor_join;
	}

	// damping factor for group by
	double DDampingFactorGroupBy() const
	{
		return m_damping_factor_groupby;
	}

	// generate default optimizer configurations
	static CStatisticsConfig* PstatsconfDefault()
	{
		return new CStatisticsConfig(0.75, 0.01, 0.75);
	}
};	// class CStatisticsConfig
}  // namespace gpopt
#endif