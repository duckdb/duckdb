//---------------------------------------------------------------------------
//	@filename:
//		CAutoOptCtxt.cpp
//
//	@doc:
//		Implementation of auto opt context
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CAutoOptCtxt.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/cost/ICostModel.h"
#include "duckdb/optimizer/cascade/eval/CConstExprEvaluatorDefault.h"
#include "duckdb/optimizer/cascade/optimizer/COptimizerConfig.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CAutoOptCtxt::CAutoOptCtxt
//
//	@doc:
//		ctor
//		Create and install default optimizer context
//
//---------------------------------------------------------------------------
CAutoOptCtxt::CAutoOptCtxt(CMemoryPool *mp, CMDAccessor *md_accessor, IConstExprEvaluator *pceeval, COptimizerConfig *optimizer_config)
{
	if (NULL == optimizer_config)
	{
		// create default statistics configuration
		optimizer_config = COptimizerConfig::PoconfDefault(mp);
	}
	if (NULL == pceeval)
	{
		// use the default constant expression evaluator which cannot evaluate any expression
		pceeval = GPOS_NEW(mp) CConstExprEvaluatorDefault();
	}
	COptCtxt *poctxt = COptCtxt::PoctxtCreate(mp, md_accessor, pceeval, optimizer_config);
	ITask::Self()->GetTls().Store(poctxt);
}

//---------------------------------------------------------------------------
//	@function:
//		CAutoOptCtxt::CAutoOptCtxt
//
//	@doc:
//		ctor
//		Create and install default optimizer context with the given cost model
//
//---------------------------------------------------------------------------
CAutoOptCtxt::CAutoOptCtxt(CMemoryPool *mp, CMDAccessor *md_accessor, IConstExprEvaluator *pceeval, ICostModel *pcm)
{
	GPOS_ASSERT(NULL != pcm);

	// create default statistics configuration
	COptimizerConfig *optimizer_config =
		COptimizerConfig::PoconfDefault(mp, pcm);

	if (NULL == pceeval)
	{
		// use the default constant expression evaluator which cannot evaluate any expression
		pceeval = GPOS_NEW(mp) CConstExprEvaluatorDefault();
	}

	COptCtxt *poctxt =
		COptCtxt::PoctxtCreate(mp, md_accessor, pceeval, optimizer_config);
	ITask::Self()->GetTls().Store(poctxt);
}

//---------------------------------------------------------------------------
//	@function:
//		CAutoOptCtxt::~CAutoOptCtxt
//
//	@doc:
//		dtor
//		uninstall optimizer context and destroy
//
//---------------------------------------------------------------------------
CAutoOptCtxt::~CAutoOptCtxt()
{
	CTaskLocalStorageObject *ptlsobj =
		ITask::Self()->GetTls().Get(CTaskLocalStorage::EtlsidxOptCtxt);
	ITask::Self()->GetTls().Remove(ptlsobj);

	GPOS_DELETE(ptlsobj);
}