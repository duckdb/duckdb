#include "duckdb/optimizer/cascade/Cascade.h"

#include "duckdb/optimizer/cascade/search/CSearchStage.h"
#include "duckdb/optimizer/cascade/memory/CAutoMemoryPool.h"
#include "duckdb/optimizer/cascade/base/CQueryContext.h"
#include "duckdb/optimizer/cascade/engine/CEngine.h"

#define AUTO_MEM_POOL(amp) CAutoMemoryPool amp(CAutoMemoryPool::ElcExc)

namespace duckdb {
	gpopt::CExpression* Cascade::Optimize(unique_ptr<LogicalOperator> plan) {
		AUTO_MEM_POOL(amp);
		CMemoryPool *mp = amp.Pmp();
		gpopt::CSearchStageArray *search_strategy_arr = LoadSearchStrategy(mp);
		
		gpopt::CExpression* pexpr = adaptator(plan);
		// pdrgpul: the array of query output column reference id
		// pdrgpmdname: the array of output column names
		gpopt::CQueryContext *pqc = gpopt::CQueryContext::PqcGenerate(mp, pexpr, pdrgpul, pdrgpmdname, true);
		
		gpopt::CEngine eng(mp);
		eng.Init(pqc, search_strategy_arr);
		eng.Optimize();

		GPOS_CHECK_ABORT;

		gpopt::CExpression *pexprPlan = eng.PexprExtractPlan();
		(void) pexprPlan->PrppCompute(mp, pqc->Prpp());

		CheckCTEConsistency(mp, pexprPlan);

		PrintQueryOrPlan(mp, pexprPlan);

		GPOS_CHECK_ABORT;

		return pexprPlan;
	}

	gpopt::CSearchStageArray* LoadSearchStrategy(CMemoryPool *mp)
	{
		gpopt::CSearchStageArray *search_strategy_arr = NULL;
		return search_strategy_arr;
	}
}