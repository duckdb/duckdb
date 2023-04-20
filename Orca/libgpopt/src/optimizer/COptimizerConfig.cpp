//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//
//	@filename:
//		COptimizerConfig.cpp
//
//	@doc:
//		Implementation of configuration used by the optimizer
//---------------------------------------------------------------------------

#include "gpopt/optimizer/COptimizerConfig.h"

#include "gpos/base.h"
#include "gpos/common/CBitSetIter.h"
#include "gpos/io/COstreamFile.h"

#include "gpopt/cost/ICostModel.h"
#include "naucrates/dxl/CCostModelConfigSerializer.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		COptimizerConfig::COptimizerConfig
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
COptimizerConfig::COptimizerConfig(CEnumeratorConfig *pec,
								   CStatisticsConfig *stats_config,
								   CCTEConfig *pcteconf, ICostModel *cost_model,
								   CHint *phint, CWindowOids *pwindowoids)
	: m_enumerator_cfg(pec),
	  m_stats_conf(stats_config),
	  m_cte_conf(pcteconf),
	  m_cost_model(cost_model),
	  m_hint(phint),
	  m_window_oids(pwindowoids)
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
COptimizerConfig *
COptimizerConfig::PoconfDefault(CMemoryPool *mp)
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
COptimizerConfig *
COptimizerConfig::PoconfDefault(CMemoryPool *mp, ICostModel *pcm)
{
	GPOS_ASSERT(NULL != pcm);

	return GPOS_NEW(mp) COptimizerConfig(
		GPOS_NEW(mp) CEnumeratorConfig(mp, 0 /*plan_id*/, 0 /*ullSamples*/),
		CStatisticsConfig::PstatsconfDefault(mp),
		CCTEConfig::PcteconfDefault(mp), pcm, CHint::PhintDefault(mp),
		CWindowOids::GetWindowOids(mp));
}

//---------------------------------------------------------------------------
//	@function:
//		COptimizerConfig::Serialize
//
//	@doc:
//		Serialize optimizer configuration
//
//---------------------------------------------------------------------------
void
COptimizerConfig::Serialize(CMemoryPool *mp, CXMLSerializer *xml_serializer,
							CBitSet *pbsTrace) const
{
	GPOS_ASSERT(NULL != xml_serializer);
	GPOS_ASSERT(NULL != pbsTrace);

	xml_serializer->OpenElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix),
		CDXLTokens::GetDXLTokenStr(EdxltokenOptimizerConfig));

	xml_serializer->OpenElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix),
		CDXLTokens::GetDXLTokenStr(EdxltokenEnumeratorConfig));
	xml_serializer->AddAttribute(CDXLTokens::GetDXLTokenStr(EdxltokenPlanId),
								 m_enumerator_cfg->GetPlanId());
	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenPlanSamples),
		m_enumerator_cfg->GetPlanId());
	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenCostThreshold),
		m_enumerator_cfg->GetPlanId());
	xml_serializer->CloseElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix),
		CDXLTokens::GetDXLTokenStr(EdxltokenEnumeratorConfig));

	xml_serializer->OpenElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix),
		CDXLTokens::GetDXLTokenStr(EdxltokenStatisticsConfig));
	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenDampingFactorFilter),
		m_stats_conf->DDampingFactorFilter());
	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenDampingFactorJoin),
		m_stats_conf->DDampingFactorJoin());
	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenDampingFactorGroupBy),
		m_stats_conf->DDampingFactorGroupBy());
	xml_serializer->CloseElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix),
		CDXLTokens::GetDXLTokenStr(EdxltokenStatisticsConfig));

	xml_serializer->OpenElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix),
		CDXLTokens::GetDXLTokenStr(EdxltokenCTEConfig));
	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenCTEInliningCutoff),
		m_cte_conf->UlCTEInliningCutoff());
	xml_serializer->CloseElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix),
		CDXLTokens::GetDXLTokenStr(EdxltokenCTEConfig));

	xml_serializer->OpenElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix),
		CDXLTokens::GetDXLTokenStr(EdxltokenWindowOids));
	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenOidRowNumber),
		m_window_oids->OidRowNumber());
	xml_serializer->AddAttribute(CDXLTokens::GetDXLTokenStr(EdxltokenOidRank),
								 m_window_oids->OidRank());
	xml_serializer->CloseElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix),
		CDXLTokens::GetDXLTokenStr(EdxltokenWindowOids));

	CCostModelConfigSerializer cmcSerializer(m_cost_model);
	cmcSerializer.Serialize(*xml_serializer);

	xml_serializer->OpenElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix),
		CDXLTokens::GetDXLTokenStr(EdxltokenHint));
	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenMinNumOfPartsToRequireSortOnInsert),
		m_hint->UlMinNumOfPartsToRequireSortOnInsert());
	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(
			EdxltokenJoinArityForAssociativityCommutativity),
		m_hint->UlJoinArityForAssociativityCommutativity());
	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenArrayExpansionThreshold),
		m_hint->UlArrayExpansionThreshold());
	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenJoinOrderDPThreshold),
		m_hint->UlJoinOrderDPLimit());
	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenBroadcastThreshold),
		m_hint->UlBroadcastThreshold());
	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenEnforceConstraintsOnDML),
		m_hint->FEnforceConstraintsOnDML());
	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenPushGroupByBelowSetopThreshold),
		m_hint->UlPushGroupByBelowSetopThreshold());
	xml_serializer->AddAttribute(
		CDXLTokens::GetDXLTokenStr(EdxltokenXformBindThreshold),
		m_hint->UlXformBindThreshold());
	xml_serializer->CloseElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix),
		CDXLTokens::GetDXLTokenStr(EdxltokenHint));

	// Serialize traceflags represented in bitset into stream
	gpos::CBitSetIter bsi(*pbsTrace);
	CWStringDynamic wsTraceFlags(mp);
	for (ULONG ul = 0; bsi.Advance(); ul++)
	{
		if (0 < ul)
		{
			wsTraceFlags.AppendCharArray(",");
		}

		wsTraceFlags.AppendFormat(GPOS_WSZ_LIT("%d"), bsi.Bit());
	}

	xml_serializer->OpenElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix),
		CDXLTokens::GetDXLTokenStr(EdxltokenTraceFlags));
	xml_serializer->AddAttribute(CDXLTokens::GetDXLTokenStr(EdxltokenValue),
								 &wsTraceFlags);
	xml_serializer->CloseElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix),
		CDXLTokens::GetDXLTokenStr(EdxltokenTraceFlags));

	xml_serializer->CloseElement(
		CDXLTokens::GetDXLTokenStr(EdxltokenNamespacePrefix),
		CDXLTokens::GetDXLTokenStr(EdxltokenOptimizerConfig));
}

// EOF
