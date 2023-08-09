//---------------------------------------------------------------------------
//	@filename:
//		CQueryContext.h
//
//	@doc:
//		A container for query-specific input objects to the optimizer
//---------------------------------------------------------------------------
#ifndef CQueryContext_H
#define CQueryContext_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/cascade/base/CReqdPropPlan.h"
#include "duckdb/optimizer/cascade/base/CReqdPropRelational.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"

using namespace gpos;
using namespace duckdb;

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CQueryContext
//
//	@doc:
//		Query specific information that optimizer receives as input
//		representing the requirements that need to be satisfied by the final
//		plan. This includes:
//		- Input logical expression
//		- Required columns
//		- Required plan (physical) properties at the top level of the query.
//		  This will include sort order, rewindability etc requested by the entire
//		  query.
//
//		The function CQueryContext::PqcGenerate() is the main routine that
//		generates a query context object for a given logical expression and
//		required output columns. See there for more details of how
//		CQueryContext is constructed.
//
//		NB: One instance of CQueryContext is created per query. It is then used
//		to initialize the CEngine.
//
//
//---------------------------------------------------------------------------
class CQueryContext
{
public:
	// required plan properties in optimizer's produced plan
	CReqdPropPlan* m_prpp;

	// required array of output columns
	duckdb::vector<ColumnBinding> m_pdrgpcr;

	// array of output column names
	duckdb::vector<std::string> m_pdrgpmdname;

	// logical expression tree to be optimized
	duckdb::unique_ptr<Operator> m_pexpr;

	// should statistics derivation take place
	bool m_fDeriveStats;

	// return top level operator in the given expression
	static LogicalOperator* PopTop(LogicalOperator* pexpr);

public:
	// ctor
	CQueryContext(duckdb::unique_ptr<Operator> pexpr, CReqdPropPlan* prpp, duckdb::vector<ColumnBinding> colref_array, duckdb::vector<std::string> pdrgpmdname, bool fDeriveStats);

	// no copy ctor
	CQueryContext(const CQueryContext &) = delete;

	// dtor
	virtual ~CQueryContext();

	bool FDeriveStats() const
	{
		return m_fDeriveStats;
	}

	//---------------------------------------------------------------------------
	//  @function:
	//    PqcGenerate
	//
	//  @doc:
	//    Generate the query context for the given expression and array of output column ref ids.
	//  
	//  @inputs:
	//    Operator* pexpr, expression representing the query
	//    ULongPtrArray* pdrgpulQueryOutputColRefId, array of output column reference id
	//    CMDNameArray* pdrgpmdname, array of output column names
	//
	//  @output:
	//    CQueryContext
	//
	//---------------------------------------------------------------------------
	static CQueryContext* PqcGenerate(duckdb::unique_ptr<Operator> pexpr, duckdb::vector<ULONG*> pdrgpulQueryOutputColRefId, duckdb::vector<std::string> pdrgpmdname, bool fDeriveStats);
};	// class CQueryContext
}  // namespace gpopt
#endif
