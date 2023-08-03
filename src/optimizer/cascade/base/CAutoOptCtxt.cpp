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
#include "duckdb/optimizer/cascade/task/CTask.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@function:
//		CAutoOptCtxt::CAutoOptCtxt
//
//	@doc:
//		ctor
//		Create and install default optimizer context
//
//---------------------------------------------------------------------------
CAutoOptCtxt::CAutoOptCtxt(IConstExprEvaluator* pceeval, COptimizerConfig* optimizer_config)
{
	if (NULL == optimizer_config)
	{
		// create default statistics configuration
		optimizer_config = COptimizerConfig::PoconfDefault();
	}
	if (NULL == pceeval)
	{
		// use the default constant expression evaluator which cannot evaluate any expression
		pceeval = new CConstExprEvaluatorDefault();
	}
	duckdb::unique_ptr<COptCtxt> poctxt = COptCtxt::PoctxtCreate(pceeval, optimizer_config);
	CTask::Self()->GetTls().Store(std::move(poctxt));
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
CAutoOptCtxt::CAutoOptCtxt(IConstExprEvaluator* pceeval, ICostModel* pcm)
{
	// create default statistics configuration
	COptimizerConfig* optimizer_config = COptimizerConfig::PoconfDefault(pcm);
	if (NULL == pceeval)
	{
		// use the default constant expression evaluator which cannot evaluate any expression
		pceeval = new CConstExprEvaluatorDefault();
	}
	duckdb::unique_ptr<COptCtxt> poctxt = COptCtxt::PoctxtCreate(pceeval, optimizer_config);
	duckdb::unique_ptr<CTaskLocalStorageObject> tl = unique_ptr_cast<COptCtxt, CTaskLocalStorageObject>(std::move(poctxt));
	CTask::Self()->GetTls().Store(std::move(tl));
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
	// CTaskLocalStorageObject* ptlsobj = CTask::Self()->GetTls().Get(EtlsidxOptCtxt);
	// CTask::Self()->GetTls().Remove(ptlsobj);
}
}