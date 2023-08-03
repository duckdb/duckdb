//---------------------------------------------------------------------------
//	@filename:
//		Cascade.cpp
//
//	@doc:
//		Implementation of cascade optimizer
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/Cascade.h"
#include "duckdb/optimizer/cascade/base/CAutoOptCtxt.h"
#include "duckdb/optimizer/cascade/base/CQueryContext.h"
#include "duckdb/optimizer/cascade/search/CSearchStage.h"
#include "duckdb/optimizer/cascade/engine/CEngine.h"
#include "duckdb/optimizer/cascade/task/CWorkerPoolManager.h"
#include "duckdb/optimizer/cascade/task/CAutoTaskProxy.h"
#include "duckdb/optimizer/cascade/xforms/CXformFactory.h"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"

#define AUTO_MEM_POOL(amp) CAutoMemoryPool amp(CAutoMemoryPool::ElcExc)

namespace duckdb {
	using namespace gpos;
	using namespace gpopt;

	duckdb::unique_ptr<PhysicalOperator> Cascade::Optimize(duckdb::unique_ptr<LogicalOperator> plan)
	{
		auto &profiler = QueryProfiler::Get(context);
		// first resolve column references
		profiler.StartPhase("column_binding");
		ColumnBindingResolver resolver;
		resolver.VisitOperator(*plan);
		profiler.EndPhase();
		// now resolve types of all the operators
		profiler.StartPhase("resolve_types");
		plan->ResolveOperatorTypes();
		profiler.EndPhase();
		// extract dependencies from the logical plan
		// DependencyExtractor extractor(dependencies);
		// extractor.VisitOperator(*plan);
		if (GPOS_OK != CWorkerPoolManager::Init())
		{
			return NULL;
		}
		if (GPOS_OK != CXformFactory::Init())
		{
			return NULL;
		}
		CWorkerPoolManager* pwpm = CWorkerPoolManager::m_worker_pool_manager.get();
		// check if worker pool is initialized
		if (nullptr == CWorkerPoolManager::m_worker_pool_manager)
		{
			return NULL;
		}
		void* pvStackStart = &pwpm;
		duckdb::unique_ptr<CWorker> wrkr = make_uniq<CWorker>(GPOS_WORKER_STACK_SIZE, (ULONG_PTR) pvStackStart);
		CWorkerPoolManager::m_worker_pool_manager->RegisterWorker(std::move(wrkr));
		CAutoTaskProxy atp(pwpm, true);
		CTask* ptsk = atp.Create(NULL, NULL);
		// init TLS
		ptsk->GetTls().Reset();
		atp.Execute(ptsk);
		vector<CSearchStage*> search_strategy_arr;
		IConstExprEvaluator* pceeval = nullptr;
		COptimizerConfig* optimizer_config = nullptr;
		CAutoOptCtxt aoc(pceeval, optimizer_config);
		duckdb::vector<ULONG*> pdrgpul;
		ULONG x = 0;
		pdrgpul.emplace_back(&x);
		duckdb::vector<std::string> pdrgpmdname;
		std::string str = "A";
		pdrgpmdname.push_back(str);
		CQueryContext* pqc = CQueryContext::PqcGenerate(std::move(plan), pdrgpul, pdrgpmdname, true);
		CEngine eng;
		eng.Init(pqc, search_strategy_arr);
		eng.Optimize();
		duckdb::unique_ptr<PhysicalOperator> pexprPlan = duckdb::unique_ptr<PhysicalOperator>((PhysicalOperator*)eng.PssPrevious()->m_pexprBest.release());
		/* I comment here */
		// CExpression* pexprPlan = eng.PexprExtractPlan();
		// CheckCTEConsistency(pexprPlan);
		// PrintQueryOrPlan(pexprPlan);
		// (void) pexprPlan->PrppCompute(pqc->m_prpp);
		atp.DestroyAll();
		wrkr.release();
		return pexprPlan;
	}
}