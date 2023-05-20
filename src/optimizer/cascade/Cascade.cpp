#include "duckdb/optimizer/cascade/Cascade.h"
#include "duckdb/optimizer/cascade/base/CAutoOptCtxt.h"
#include "duckdb/optimizer/cascade/memory/CMemoryPoolManager.h"
#include "duckdb/optimizer/cascade/search/CSearchStage.h"
#include "duckdb/optimizer/cascade/memory/CAutoMemoryPool.h"
#include "duckdb/optimizer/cascade/base/CQueryContext.h"
#include "duckdb/optimizer/cascade/engine/CEngine.h"
#include "duckdb/optimizer/cascade/task/CWorkerPoolManager.h"
#include "duckdb/optimizer/cascade/error/CMessageRepository.h"
#include "duckdb/optimizer/cascade/task/CAutoTaskProxy.h"

#define AUTO_MEM_POOL(amp) CAutoMemoryPool amp(CAutoMemoryPool::ElcExc)

namespace duckdb {
	using namespace gpos;
	using namespace gpmd;
	using namespace gpopt;

	unique_ptr<PhysicalOperator> Cascade::Optimize(unique_ptr<LogicalOperator> plan)
	{
		if (GPOS_OK != CMemoryPoolManager::Init())
		{
			return NULL;
		}
		if (GPOS_OK != CWorkerPoolManager::Init())
		{
			CMemoryPoolManager::GetMemoryPoolMgr()->Shutdown();
			return NULL;
		}
		if (GPOS_OK != CMessageRepository::Init())
		{
			CWorkerPoolManager::WorkerPoolManager()->Shutdown();
			CMemoryPoolManager::GetMemoryPoolMgr()->Shutdown();
			return NULL;
		}
		/*
		if (GPOS_OK != gpos::CCacheFactory::Init())
		{
			return;
		}
		*/
		CWorkerPoolManager *pwpm = CWorkerPoolManager::WorkerPoolManager();
		// check if worker pool is initialized
		if (NULL == pwpm)
		{
			return NULL;
		}
		void* pvStackStart = &pwpm;
		// put worker to stack - main thread has id '0'
		CWorker wrkr(GPOS_WORKER_STACK_SIZE, (ULONG_PTR) pvStackStart);
		AUTO_MEM_POOL(amp);
		CMemoryPool* mp = amp.Pmp();
		CAutoTaskProxy atp(mp, pwpm, true);
		CTask *ptsk = atp.Create(NULL, NULL);
		// init TLS
		ptsk->GetTls().Reset(mp);
		atp.Execute(ptsk);
		CSearchStageArray* search_strategy_arr = LoadSearchStrategy(mp);
		CMDProviderArray* relcache_provider = GPOS_NEW(mp) CMDProviderArray(mp);
		CMDAccessor mda(mp, NULL, NULL, relcache_provider);
		CAutoOptCtxt aoc(mp, &mda, (IConstExprEvaluator*)NULL, (COptimizerConfig*)NULL);
		CExpression* pexpr = Orca2Duck(std::move(plan));
		ULongPtrArray* pdrgpul = GPOS_NEW(mp) ULongPtrArray(mp);
		ULONG x = 0;
		pdrgpul->Append(&x);
		CMDNameArray* pdrgpmdname = GPOS_NEW(mp) CMDNameArray(mp);
		WCHAR str[] = L"A";
		CWStringConst* cwstr = GPOS_NEW(mp) CWStringConst(str);
		CMDName* mdname = GPOS_NEW(mp) CMDName(mp, cwstr);
		pdrgpmdname->Append(mdname);
		// pdrgpul: the array of query output column reference id
		// pdrgpmdname: the array of output column names
		CQueryContext* pqc = CQueryContext::PqcGenerate(mp, pexpr, pdrgpul, pdrgpmdname, true);
		CEngine eng(mp);
		eng.Init(pqc, search_strategy_arr);
		eng.Optimize();

		GPOS_CHECK_ABORT;

		CExpression* pexprPlan = eng.PexprExtractPlan();
		(void) pexprPlan->PrppCompute(mp, pqc->Prpp());

		//CheckCTEConsistency(mp, pexprPlan);

		//PrintQueryOrPlan(mp, pexprPlan);

		GPOS_CHECK_ABORT;
		unique_ptr<PhysicalOperator> final = Duck2Orca(pexprPlan);
		return final;
	}

	CSearchStageArray* Cascade::LoadSearchStrategy(CMemoryPool *mp)
	{
		CSearchStageArray *search_strategy_arr = NULL;
		return search_strategy_arr;
	}

	CExpression* Cascade::Orca2Duck(unique_ptr<LogicalOperator> plan) {
		return NULL;
	}

	unique_ptr<PhysicalOperator> Cascade::Duck2Orca(CExpression* Plan) {
		return NULL;
	}
}