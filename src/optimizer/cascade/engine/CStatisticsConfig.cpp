//---------------------------------------------------------------------------
//	@filename:
//		CStatisticsConfig.cpp
//
//	@doc:
//		Implementation of statistics context
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/engine/CStatisticsConfig.h"
#include "duckdb/optimizer/cascade/base.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@function:
//		CStatisticsConfig::CStatisticsConfig
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CStatisticsConfig::CStatisticsConfig(double damping_factor_filter, double damping_factor_join, double damping_factor_groupby)
	: m_damping_factor_filter(damping_factor_filter), m_damping_factor_join(damping_factor_join), m_damping_factor_groupby(damping_factor_groupby)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsConfig::~CStatisticsConfig
//
//	@doc:
//		dtor
//		Does not de-allocate memory pool!
//
//---------------------------------------------------------------------------
CStatisticsConfig::~CStatisticsConfig()
{
}
}