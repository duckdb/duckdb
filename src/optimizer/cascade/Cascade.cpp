#include "duckdb/optimizer/cascade/Cascade.h"
#include "duckdb/optimizer/cascade/base/CAutoOptCtxt.h"
#include "duckdb/optimizer/cascade/base/CQueryContext.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/memory/CMemoryPoolManager.h"
#include "duckdb/optimizer/cascade/search/CSearchStage.h"
#include "duckdb/optimizer/cascade/memory/CAutoMemoryPool.h"
#include "duckdb/optimizer/cascade/engine/CEngine.h"
#include "duckdb/optimizer/cascade/task/CWorkerPoolManager.h"
#include "duckdb/optimizer/cascade/error/CMessageRepository.h"
#include "duckdb/optimizer/cascade/task/CAutoTaskProxy.h"
#include "duckdb/optimizer/cascade/operators/ops.h"
#include "duckdb/optimizer/cascade/test/CMDIdTest.h"
#include "duckdb/optimizer/cascade/test/MyTypeInt4.h"
#include "duckdb/optimizer/cascade/test/MyDatumInt4.h"
#include "duckdb/optimizer/cascade/xforms/CXformFactory.h"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/enums/expression_type.hpp"

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
		if (GPOS_OK != CXformFactory::Init())
		{
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
		CSearchStageArray* search_strategy_arr = NULL;
		CMDProviderArray* relcache_provider = GPOS_NEW(mp) CMDProviderArray(mp);
		CMDAccessor mda(mp, NULL, NULL, relcache_provider);
		CAutoOptCtxt aoc(mp, &mda, (IConstExprEvaluator*)NULL, (COptimizerConfig*)NULL);
		// Here we start simulation
		// Let's start to create the first table foo
		// WCHAR str1[] = L"foo";
		// CWStringConst* cwstr1 = GPOS_NEW(mp) CWStringConst(str1);
		// CMDIdTest* mid1 = GPOS_NEW(mp) CMDIdTest(1);
		// CTableDescriptor* ptabdesc1 = GPOS_NEW(mp) CTableDescriptor(mp, mid1, CName(cwstr1), false, IMDRelation::Ereldistrpolicy::EreldistrMasterOnly, IMDRelation::Erelstoragetype::ErelstorageAppendOnlyCols, (ULONG)0);
		// Create the column of the first table
		// WCHAR str1_1[] = L"id";
		// CWStringConst* cwstr1_1 = GPOS_NEW(mp) CWStringConst(str1_1);
		// IMDType* pcoltype = GPOS_NEW(mp) MyTypeInt4(mp);
		// CColumnDescriptor* pcoldesc1_1 = GPOS_NEW(mp) CColumnDescriptor(mp, pcoltype, 0, CName(cwstr1_1), 1, false, 32);
		// Add this col to the first table
		// ptabdesc1->AddColumn(pcoldesc1_1);
		// Create logical operator
		// CLogicalGet* pop1 = GPOS_NEW(mp) CLogicalGet(mp, GPOS_NEW(mp) CName(mp, CName(cwstr1)), ptabdesc1);
		// CExpression* pexpr1 = GPOS_NEW(mp) CExpression(mp, pop1);
		// const CColRef* pcr1 = pexpr1->DeriveOutputColumns()->PcrAny();

		// Let's start to create the second table goo
		// WCHAR str2[] = L"goo";
		// CWStringConst* cwstr2 = GPOS_NEW(mp) CWStringConst(str2);
		// CMDIdTest* mid2 = GPOS_NEW(mp) CMDIdTest(2);
		// CTableDescriptor* ptabdesc2 = GPOS_NEW(mp) CTableDescriptor(mp, mid2, CName(cwstr2), false, IMDRelation::Ereldistrpolicy::EreldistrMasterOnly, IMDRelation::Erelstoragetype::ErelstorageAppendOnlyCols, (ULONG)0);
		// Create the column of the second table
		// WCHAR str2_1[] = L"id";
		// CWStringConst* cwstr2_1 = GPOS_NEW(mp) CWStringConst(str2_1);
		// CColumnDescriptor* pcoldesc2_1 = GPOS_NEW(mp) CColumnDescriptor(mp, pcoltype, 0, CName(cwstr2_1), 1, false, 32);
		// Add this col to the first table
		// ptabdesc2->AddColumn(pcoldesc2_1);
		// Create logical operator
		// CLogicalGet* pop2 = GPOS_NEW(mp) CLogicalGet(mp, GPOS_NEW(mp) CName(mp, CName(cwstr2)), ptabdesc2);
		// CExpression* pexpr2 = GPOS_NEW(mp) CExpression(mp, pop2);
		// const CColRef* pcr2 = pexpr2->DeriveOutputColumns()->PcrAny();
		// CExpression* pexprPred = CUtils::PexprScalarCmp(mp, pcr1, pcr2, IMDType::EcmptEq);
		// pexpr = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(mp, pexpr1, pexpr2, pexprPred);
		// Create the output format
		ULongPtrArray* pdrgpul = GPOS_NEW(mp) ULongPtrArray(mp);
		ULONG x = 0;
		pdrgpul->Append(&x);
		CMDNameArray* pdrgpmdname = GPOS_NEW(mp) CMDNameArray(mp);
		WCHAR str[] = L"A";
		CWStringConst* cwstr = GPOS_NEW(mp) CWStringConst(str);
		CMDName* mdname = GPOS_NEW(mp) CMDName(mp, cwstr);
		pdrgpmdname->Append(mdname);
		// And we end simulation here

		CQueryContext* pqc = CQueryContext::PqcGenerate(mp, plan.get(), pdrgpul, pdrgpmdname, true);
		CEngine eng(mp);
		eng.Init(pqc, search_strategy_arr);
		eng.Optimize();
		GPOS_CHECK_ABORT;
		unique_ptr<PhysicalOperator> pexprPlan = std::move(eng.PssPrevious()->m_pexprBest);
		/* I comment here */
		// CExpression* pexprPlan = eng.PexprExtractPlan();
		// CheckCTEConsistency(mp, pexprPlan);
		// PrintQueryOrPlan(mp, pexprPlan);
		(void) pexprPlan->PrppCompute(mp, pqc->Prpp());
		GPOS_CHECK_ABORT;
		atp.DestroyAll();
		return pexprPlan;
	}
}