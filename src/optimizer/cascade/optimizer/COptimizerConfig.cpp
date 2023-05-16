//---------------------------------------------------------------------------
//	@filename:
//		COptimizerConfig.cpp
//
//	@doc:
//		Implementation of configuration used by the optimizer
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/optimizer/COptimizerConfig.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CBitSetIter.h"
#include "duckdb/optimizer/cascade/io/COstreamFile.h"
#include "duckdb/optimizer/cascade/cost/ICostModel.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		COptimizerConfig::COptimizerConfig
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
COptimizerConfig::COptimizerConfig(CEnumeratorConfig *pec, CStatisticsConfig *stats_config, CCTEConfig *pcteconf, ICostModel *cost_model, CHint *phint, CWindowOids *pwindowoids)
	: m_enumerator_cfg(pec), m_stats_conf(stats_config), m_cte_conf(pcteconf), m_cost_model(cost_model), m_hint(phint), m_window_oids(pwindowoids)
{
	GPOS_ASSERT(NULL != pec);
	GPOS_ASSERT(NULL != stats_config);
	GPOS_ASSERT(NULL != pcteconf);
	GPOS_ASSERT(NULL != m_cost_model);
	GPOS_ASSERT(NULL != phint);
	GPOS_ASSERT(NULL != m_window_oids);
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
	m_enumerator_cfg->Release();
	m_stats_conf->Release();
	m_cte_conf->Release();
	m_cost_model->Release();
	m_hint->Release();
	m_window_oids->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizerConfig::PocDefault
//
//	@doc:
//		Default optimizer configuration
//
//---------------------------------------------------------------------------
COptimizerConfig* COptimizerConfig::PoconfDefault(CMemoryPool *mp)
{
	return GPOS_NEW(mp) COptimizerConfig(
		GPOS_NEW(mp) CEnumeratorConfig(mp, 0 /*plan_id*/, 0 /*ullSamples*/),
		CStatisticsConfig::PstatsconfDefault(mp),
		CCTEConfig::PcteconfDefault(mp), ICostModel::PcmDefault(mp),
		CHint::PhintDefault(mp), CWindowOids::GetWindowOids(mp));
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizerConfig::PocDefault
//
//	@doc:
//		Default optimizer configuration with the given cost model
//
//---------------------------------------------------------------------------
COptimizerConfig* COptimizerConfig::PoconfDefault(CMemoryPool *mp, ICostModel *pcm)
{
	GPOS_ASSERT(NULL != pcm);

	return GPOS_NEW(mp) COptimizerConfig(
		GPOS_NEW(mp) CEnumeratorConfig(mp, 0 /*plan_id*/, 0 /*ullSamples*/),
		CStatisticsConfig::PstatsconfDefault(mp),
		CCTEConfig::PcteconfDefault(mp), pcm, CHint::PhintDefault(mp),
		CWindowOids::GetWindowOids(mp));
}