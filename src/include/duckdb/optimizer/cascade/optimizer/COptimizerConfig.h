//---------------------------------------------------------------------------
//	@filename:
//		COptimizerConfig.h
//
//	@doc:
//		Configurations used by the optimizer
//---------------------------------------------------------------------------
#ifndef GPOPT_COptimizerConfig_H
#define GPOPT_COptimizerConfig_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/engine/CEnumeratorConfig.h"
#include "duckdb/optimizer/cascade/engine/CHint.h"
#include "duckdb/optimizer/cascade/engine/CStatisticsConfig.h"

namespace gpopt
{
using namespace gpos;

// forward decl
class ICostModel;

//---------------------------------------------------------------------------
//	@class:
//		COptimizerConfig
//
//	@doc:
//		Configuration parameters of the optimizer including damping factors used
//		during statistics derivation, CTE inlining cut-off threshold, Id of plan to
//		be extracted (if plan enumeration is enabled), number of plans to be sampled
//		from the space (if plan sampling is enabled) etc.
//
//		Most of these configurations can be changed from outside ORCA through
//		GUCs. They are also included in optimizer’s minidumps under
//		<dxl:OptimizerConfig> element
//
//---------------------------------------------------------------------------
class COptimizerConfig
{
public:
	// plan enumeration configuration
	CEnumeratorConfig* m_enumerator_cfg;

	// statistics configuration
	CStatisticsConfig* m_stats_conf;

	// cost model configuration
	ICostModel* m_cost_model;

	// hint configuration
	CHint* m_hint;

public:
	// ctor
	COptimizerConfig(CEnumeratorConfig* pec, CStatisticsConfig* stats_config, ICostModel* cost_model, CHint* phint);

	// dtor
	virtual ~COptimizerConfig();

	// generate default optimizer configurations
	static COptimizerConfig* PoconfDefault();

	// generate default optimizer configurations with the given cost model
	static COptimizerConfig* PoconfDefault(ICostModel* pcm);
};	// class COptimizerConfig
}  // namespace gpopt
#endif