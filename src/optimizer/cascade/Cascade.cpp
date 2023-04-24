#include "duckdb/optimizer/cascade/Cascade.h"

#include "duckdb/optimizer/cascade/search/CSearchStageArray.h"
#include "duckdb/optimizer/cascade/memory/CAutoMemoryPool.h"

#define AUTO_MEM_POOL(amp) CAutoMemoryPool amp(CAutoMemoryPool::ElcExc)

Cascade::Optimize(unique_ptr<LogialOperator> plan) {
	AUTO_MEM_POOL(amp);
	CMemoryPool *mp = amp.Pmp();
	CSearchStageArray *search_strategy_arr = LoadSearchStrategy(mp, optimizer_search_strategy_path);
	
	// pdrgpul: the array of query output column reference id
	// pdrgpmdname: the array of output column names
	CQueryContext *pqc = CQueryContext::PqcGenerate(mp, plan, pdrgpul, pdrgpmdname, true);
	
	CEngine eng(mp);
	eng.Init(pqc, search_stage_array);
	eng.Optimize();

	GPOS_CHECK_ABORT;

	CExpression *pexprPlan = eng.PexprExtractPlan();
	(void) pexprPlan->PrppCompute(mp, pqc->Prpp());

	CheckCTEConsistency(mp, pexprPlan);

	PrintQueryOrPlan(mp, pexprPlan);

	GPOS_CHECK_ABORT;

	return pexprPlan;
}

CSearchStageArray* COptTasks::LoadSearchStrategy(CMemoryPool *mp, char *path)
{
	CSearchStageArray *search_strategy_arr = NULL;
	return search_strategy_arr;
}
