//---------------------------------------------------------------------------
//	@filename:
//		COptimizerConfig.cpp
//
//	@doc:
//		Implementation of configuration used by the optimizer
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/optimizer/COptimizerConfig.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/io/COstreamFile.h"
#include "duckdb/optimizer/cascade/cost/ICostModel.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@function:
//		COptimizerConfig::COptimizerConfig
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
COptimizerConfig::COptimizerConfig(CEnumeratorConfig* pec, CStatisticsConfig* stats_config, ICostModel* cost_model, CHint* phint)
	: m_enumerator_cfg(pec), m_stats_conf(stats_config), m_cost_model(cost_model), m_hint(phint)
{
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizerConfig::~COptimizerConfig
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
COptimizerConfig::~COptimizerConfig()
{
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizerConfig::PocDefault
//
//	@doc:
//		Default optimizer configuration
//
//---------------------------------------------------------------------------
COptimizerConfig* COptimizerConfig::PoconfDefault()
{
	return new COptimizerConfig(new CEnumeratorConfig(0, 0), CStatisticsConfig::PstatsconfDefault(), ICostModel::PcmDefault(), CHint::PhintDefault());
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizerConfig::PocDefault
//
//	@doc:
//		Default optimizer configuration with the given cost model
//
//---------------------------------------------------------------------------
COptimizerConfig* COptimizerConfig::PoconfDefault(ICostModel* pcm)
{
	return new COptimizerConfig(new CEnumeratorConfig(0, 0), CStatisticsConfig::PstatsconfDefault(), pcm, CHint::PhintDefault());
}
}