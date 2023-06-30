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
#include "duckdb/optimizer/cascade/task/CTaskLocalStorageObject.h"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/optimizer/cascade/base/CCTEInfo.h"
#include "duckdb/optimizer/cascade/base/CColumnFactory.h"
#include "duckdb/optimizer/cascade/base/IComparator.h"
#include "duckdb/optimizer/cascade/mdcache/CMDAccessor.h"
#include "duckdb/optimizer/cascade/traceflags/traceflags.h"

namespace gpopt
{
using namespace gpos;
using namespace duckdb;

// forward declarations
class CColRefSet;
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
private:
	// private copy ctor
	COptCtxt(COptCtxt &);

	// shared memory pool
	CMemoryPool *m_mp;

	// column factory
	CColumnFactory *m_pcf;

	// metadata accessor;
	CMDAccessor *m_pmda;

	// cost model
	ICostModel *m_cost_model;

	// constant expression evaluator
	IConstExprEvaluator *m_pceeval;

	// comparator between IDatum instances
	IComparator *m_pcomp;

	// atomic counter for generating part index ids
	ULONG m_auPartId;

	// global CTE information
	CCTEInfo *m_pcteinfo;

	// system columns required in query output
	CColRefArray *m_pdrgpcrSystemCols;

	// optimizer configurations
	COptimizerConfig *m_optimizer_config;

	// whether or not we are optimizing a DML query
	BOOL m_fDMLQuery;

	// value for the first valid part id
	static ULONG m_ulFirstValidPartId;

	// if there are master only tables in the query
	BOOL m_has_master_only_tables;

	// does the query contain any volatile functions or
	// functions that read/modify SQL data
	BOOL m_has_volatile_or_SQL_func;

	// does the query have replicated tables
	BOOL m_has_replicated_tables;

	// does this plan have a direct dispatchable filter
	ExpressionArray *m_direct_dispatchable_filters;

public:
	// ctor
	COptCtxt(CMemoryPool *mp, CColumnFactory *col_factory,
			 CMDAccessor *md_accessor, IConstExprEvaluator *pceeval,
			 COptimizerConfig *optimizer_config);

	// dtor
	virtual ~COptCtxt();

	// memory pool accessor
	CMemoryPool *
	Pmp() const
	{
		return m_mp;
	}

	// optimizer configurations
	COptimizerConfig *
	GetOptimizerConfig() const
	{
		return m_optimizer_config;
	}

	// are we optimizing a DML query
	BOOL
	FDMLQuery() const
	{
		return m_fDMLQuery;
	}

	// set the DML flag
	void
	MarkDMLQuery(BOOL fDMLQuery)
	{
		m_fDMLQuery = fDMLQuery;
	}

	void
	SetHasMasterOnlyTables()
	{
		m_has_master_only_tables = true;
	}

	void
	SetHasVolatileOrSQLFunc()
	{
		m_has_volatile_or_SQL_func = true;
	}

	void SetHasReplicatedTables()
	{
		m_has_replicated_tables = true;
	}

	void AddDirectDispatchableFilterCandidate(unique_ptr<Expression> filter_expression)
	{
		m_direct_dispatchable_filters->Append(std::move(filter_expression));
	}

	BOOL
	HasMasterOnlyTables() const
	{
		return m_has_master_only_tables;
	}

	BOOL
	HasVolatileOrSQLFunc() const
	{
		return m_has_volatile_or_SQL_func;
	}

	BOOL
	HasReplicatedTables() const
	{
		return m_has_replicated_tables;
	}

	ExpressionArray* GetDirectDispatchableFilters() const
	{
		return m_direct_dispatchable_filters;
	}

	BOOL OptimizeDMLQueryWithSingletonSegment() const
	{
		// A DML statement can be optimized by enforcing a gather motion on segment instead of master,
		// whenever a singleton execution is needed.
		// This optmization can not be applied if the query contains any of the following:
		// (1). master-only tables
		// (2). a volatile function
		// (3). a function SQL dataaccess: EfdaContainsSQL or EfdaReadsSQLData or EfdaModifiesSQLData
		//      In such cases, it is safe to *always* enforce gather motion on master as there is no way to determine
		//      if the SQL contains any master-only tables.
		return !GPOS_FTRACE(EopttraceDisableNonMasterGatherForDML) && FDMLQuery() && !HasMasterOnlyTables() && !HasVolatileOrSQLFunc();
	}

	// column factory accessor
	CColumnFactory* Pcf() const
	{
		return m_pcf;
	}

	// metadata accessor
	CMDAccessor* Pmda() const
	{
		return m_pmda;
	}

	// cost model accessor
	ICostModel* GetCostModel() const
	{
		return m_cost_model;
	}

	// constant expression evaluator
	IConstExprEvaluator* Pceeval()
	{
		return m_pceeval;
	}

	// comparator
	const IComparator* Pcomp()
	{
		return m_pcomp;
	}

	// cte info
	CCTEInfo* Pcteinfo()
	{
		return m_pcteinfo;
	}

	// return a new part index id
	ULONG UlPartIndexNextVal()
	{
		return m_auPartId++;
	}

	// required system columns
	CColRefArray* PdrgpcrSystemCols() const
	{
		return m_pdrgpcrSystemCols;
	}

	// set required system columns
	void SetReqdSystemCols(CColRefArray *pdrgpcrSystemCols)
	{
		GPOS_ASSERT(NULL != pdrgpcrSystemCols);
		CRefCount::SafeRelease(m_pdrgpcrSystemCols);
		m_pdrgpcrSystemCols = pdrgpcrSystemCols;
	}

	// factory method
	static COptCtxt *PoctxtCreate(CMemoryPool *mp, CMDAccessor *md_accessor, IConstExprEvaluator *pceeval, COptimizerConfig *optimizer_config);

	// shorthand to retrieve opt context from TLS
	inline static COptCtxt* PoctxtFromTLS()
	{
		return reinterpret_cast<COptCtxt *>(ITask::Self()->GetTls().Get(CTaskLocalStorage::EtlsidxOptCtxt));
	}

	// return true if all enforcers are enabled
	static BOOL FAllEnforcersEnabled();

#ifdef GPOS_DEBUG
	virtual IOstream &OsPrint(IOstream &) const;
#endif	// GPOS_DEBUG

};	// class COptCtxt
}  // namespace gpopt

#endif
