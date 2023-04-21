#include "duckdb/optimizer/cascade/Cascade.h"

Cascade::Optimize(unique_ptr<LogialOperator> plan) {
		CSearchStageArray *search_strategy_arr = LoadSearchStrategy(mp, optimizer_search_strategy_path);

		CBitSet *trace_flags = NULL;
		CBitSet *enabled_trace_flags = NULL;
		CBitSet *disabled_trace_flags = NULL;
		CDXLNode *plan_dxl = NULL;

		IMdIdArray *col_stats = NULL;
		MdidHashSet *rel_stats = NULL;
	
		trace_flags = CConfigParamMapping::PackConfigParamInBitset(mp, CXform::ExfSentinel);
		SetTraceflags(mp, trace_flags, &enabled_trace_flags, &disabled_trace_flags);

		// set up relcache MD provider
		CMDProviderRelcache *relcache_provider = GPOS_NEW(mp) CMDProviderRelcache(mp);

		// scope for MD accessor
		CMDAccessor mda(mp, CMDCache::Pcache(), default_sysid, relcache_provider);

		ULONG num_segments = gpdb::GetGPSegmentCount();
		ULONG num_segments_for_costing = optimizer_segments;
		if (0 == num_segments_for_costing)
		{
				num_segments_for_costing = num_segments;
		}

		CAutoP<CTranslatorQueryToDXL> query_to_dxl_translator;
		query_to_dxl_translator = CTranslatorQueryToDXL::QueryToDXLInstance(mp, &mda, (Query *) opt_ctxt->m_query);

		ICostModel *cost_model = GetCostModel(mp, num_segments_for_costing);
		COptimizerConfig *optimizer_config = CreateOptimizerConfig(mp, cost_model);
		CConstExprEvaluatorProxy expr_eval_proxy(mp, &mda);
		IConstExprEvaluator *expr_evaluator = GPOS_NEW(mp) CConstExprEvaluatorDXL(mp, &mda, &expr_eval_proxy);

		CDXLNode *query_dxl = query_to_dxl_translator->TranslateQueryToDXL();
		CDXLNodeArray *query_output_dxlnode_array = query_to_dxl_translator->GetQueryOutputCols();
		CDXLNodeArray *cte_dxlnode_array = query_to_dxl_translator->GetCTEs();
		GPOS_ASSERT(NULL != query_output_dxlnode_array);

		BOOL is_master_only = !optimizer_enable_motions || (!optimizer_enable_motions_masteronly_queries && !query_to_dxl_translator->HasDistributedTables());
		// See NoteDistributionPolicyOpclasses() in src/backend/gpopt/translate/CTranslatorQueryToDXL.cpp
		BOOL use_legacy_opfamilies = (query_to_dxl_translator->GetDistributionHashOpsKind() == DistrUseLegacyHashOps);
		CAutoTraceFlag atf1(EopttraceDisableMotions, is_master_only);
		CAutoTraceFlag atf2(EopttraceUseLegacyOpfamilies, use_legacy_opfamilies);

		plan_dxl = COptimizer::PdxlnOptimize(mp, &mda, query_dxl, query_output_dxlnode_array, cte_dxlnode_array, expr_evaluator, num_segments, gp_session_id, gp_command_count, search_strategy_arr, optimizer_config);

		if (opt_ctxt->m_should_serialize_plan_dxl)
		{
					// serialize DXL to xml
					CWStringDynamic plan_str(mp);
					COstreamString oss(&plan_str);
					CDXLUtils::SerializePlan(mp, oss, plan_dxl, optimizer_config->GetEnumeratorCfg()->GetPlanId(), optimizer_config->GetEnumeratorCfg()->GetPlanSpaceSize(), true /*serialize_header_footer*/, true /*indentation*/);
					opt_ctxt->m_plan_dxl = CreateMultiByteCharStringFromWCString(plan_str.GetBuffer());
		}

		// translate DXL->PlStmt only when needed
		if (opt_ctxt->m_should_generate_plan_stmt)
		{
					// always use opt_ctxt->m_query->can_set_tag as the query_to_dxl_translator->Pquery() is a mutated Query object
					// that may not have the correct can_set_tag
					opt_ctxt->m_plan_stmt = (PlannedStmt *) gpdb::CopyObject(ConvertToPlanStmtFromDXL(mp, &mda, opt_ctxt->m_query, plan_dxl, opt_ctxt->m_query->canSetTag, query_to_dxl_translator->GetDistributionHashOpsKind()));
		}

		CStatisticsConfig *stats_conf = optimizer_config->GetStatsConf();
		col_stats = GPOS_NEW(mp) IMdIdArray(mp);
		stats_conf->CollectMissingStatsColumns(col_stats);

		rel_stats = GPOS_NEW(mp) MdidHashSet(mp);
		PrintMissingStatsWarning(mp, &mda, col_stats, rel_stats);

		rel_stats->Release();
		col_stats->Release();

		expr_evaluator->Release();
		query_dxl->Release();
		optimizer_config->Release();
		plan_dxl->Release();
	
		// cleanup
		ResetTraceflags(enabled_trace_flags, disabled_trace_flags);
		CRefCount::SafeRelease(enabled_trace_flags);
		CRefCount::SafeRelease(disabled_trace_flags);
		CRefCount::SafeRelease(trace_flags);
		if (!optimizer_metadata_caching)
		{
					CMDCache::Shutdown();
		}
		return NULL;
}
