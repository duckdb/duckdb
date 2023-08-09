//---------------------------------------------------------------------------
//	@filename:
//		COptCtxt.h
//
//	@doc:
//		Optimizer context object; contains all global objects pertaining to
//		one optimization
//---------------------------------------------------------------------------
#ifndef GPOPT_COptCtxt_H
#define GPOPT_COptCtxt_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/task/CTask.h"
#include "duckdb/optimizer/cascade/task/CTaskLocalStorage.h"
#include "duckdb/common/unique_ptr.hpp"

using namespace gpos;
using namespace duckdb;

namespace gpopt
{
// forward declarations
class COptimizerConfig;
class ICostModel;
class IConstExprEvaluator;

//---------------------------------------------------------------------------
//	@class:
//		COptCtxt
//
//	@doc:
//		"Optimizer Context" is a container of global objects (mostly
//		singletons) that are needed by the optimizer.
//
//		A COptCtxt object is instantiated in COptimizer::PdxlnOptimize() via
//		COptCtxt::PoctxtCreate() and stored as a task local object. The global
//		information contained in it can be accessed by calling
//		COptCtxt::PoctxtFromTLS(), instead of passing a pointer to it all
//		around. For example to get the global CMDAccessor:
//			CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
//
//---------------------------------------------------------------------------
class COptCtxt : public CTaskLocalStorageObject
{
public:
	// cost model
	ICostModel* m_cost_model;

	// constant expression evaluator
	IConstExprEvaluator* m_pceeval;

	// atomic counter for generating part index ids
	ULONG m_auPartId;

	// optimizer configurations
	COptimizerConfig* m_optimizer_config;

	// whether or not we are optimizing a DML query
	bool m_fDMLQuery;

	// value for the first valid part id
	static ULONG m_ulFirstValidPartId;

	// if there are master only tables in the query
	bool m_has_master_only_tables;

	// does the query contain any volatile functions or
	// functions that read/modify SQL data
	bool m_has_volatile_or_SQL_func;

	// does the query have replicated tables
	bool m_has_replicated_tables;

public:
	// ctor
	COptCtxt(IConstExprEvaluator* pceeval, COptimizerConfig* optimizer_config);
	
	// copy ctor
	COptCtxt(COptCtxt &) = delete;

	// dtor
	virtual ~COptCtxt();

	// are we optimizing a DML query
	bool FDMLQuery() const
	{
		return m_fDMLQuery;
	}

	// set the DML flag
	void MarkDMLQuery(bool fDMLQuery)
	{
		m_fDMLQuery = fDMLQuery;
	}

	void SetHasMasterOnlyTables()
	{
		m_has_master_only_tables = true;
	}

	void SetHasVolatileOrSQLFunc()
	{
		m_has_volatile_or_SQL_func = true;
	}

	void SetHasReplicatedTables()
	{
		m_has_replicated_tables = true;
	}

	bool HasMasterOnlyTables() const
	{
		return m_has_master_only_tables;
	}

	bool HasVolatileOrSQLFunc() const
	{
		return m_has_volatile_or_SQL_func;
	}

	bool HasReplicatedTables() const
	{
		return m_has_replicated_tables;
	}

	bool OptimizeDMLQueryWithSingletonSegment() const
	{
		return false;
	}

	// return a new part index id
	ULONG UlPartIndexNextVal()
	{
		return m_auPartId++;
	}

	// factory method
	static duckdb::unique_ptr<COptCtxt> PoctxtCreate(IConstExprEvaluator* pceeval, COptimizerConfig* optimizer_config);

	// shorthand to retrieve opt context from TLS
	inline static COptCtxt* PoctxtFromTLS()
	{
		return (COptCtxt*)(CTask::Self()->GetTls().Get(EtlsidxOptCtxt));
	}

	// return true if all enforcers are enabled
	static bool FAllEnforcersEnabled();
};	// class COptCtxt
}  // namespace gpopt
#endif