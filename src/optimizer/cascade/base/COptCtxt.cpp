//---------------------------------------------------------------------------
//	@filename:
//		COptCtxt.cpp
//
//	@doc:
//		Implementation of optimizer context
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/cost/ICostModel.h"
#include "duckdb/optimizer/cascade/eval/IConstExprEvaluator.h"
#include "duckdb/optimizer/cascade/optimizer/COptimizerConfig.h"
#include "duckdb/common/helper.hpp"

namespace gpopt
{
// value of the first value part id
ULONG COptCtxt::m_ulFirstValidPartId = 1;

//---------------------------------------------------------------------------
//	@function:
//		COptCtxt::COptCtxt
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
COptCtxt::COptCtxt(IConstExprEvaluator* pceeval, COptimizerConfig* optimizer_config)
	: CTaskLocalStorageObject(EtlsidxOptCtxt), m_pceeval(pceeval), m_auPartId(m_ulFirstValidPartId), m_optimizer_config(optimizer_config), m_fDMLQuery(false), m_has_master_only_tables(false), m_has_volatile_or_SQL_func(false), m_has_replicated_tables(false)
{
	m_cost_model = optimizer_config->m_cost_model;
}

//---------------------------------------------------------------------------
//	@function:
//		COptCtxt::~COptCtxt
//
//	@doc:
//		dtor
//		Does not de-allocate memory pool!
//
//---------------------------------------------------------------------------
COptCtxt::~COptCtxt()
{
}

//---------------------------------------------------------------------------
//	@function:
//		COptCtxt::PoctxtCreate
//
//	@doc:
//		Factory method for optimizer context
//
//---------------------------------------------------------------------------
duckdb::unique_ptr<COptCtxt> COptCtxt::PoctxtCreate(IConstExprEvaluator* pceeval, COptimizerConfig* optimizer_config)
{
	// CONSIDER:  - 1/5/09; allocate column factory out of given mem pool
	// instead of having it create its own;
	duckdb::unique_ptr<COptCtxt> poctxt = make_uniq<COptCtxt>(pceeval, optimizer_config);
	return poctxt;
}

//---------------------------------------------------------------------------
//	@function:
//		COptCtxt::FAllEnforcersEnabled
//
//	@doc:
//		Return true if all enforcers are enabled
//
//---------------------------------------------------------------------------
bool COptCtxt::FAllEnforcersEnabled()
{
	return true;
}
}