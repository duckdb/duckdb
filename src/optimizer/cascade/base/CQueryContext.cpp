//---------------------------------------------------------------------------
//	@filename:
//		CQueryContext.cpp
//
//	@doc:
//		Implementation of optimization context
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CQueryContext.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"

using namespace gpopt;
using namespace duckdb;
//---------------------------------------------------------------------------
//	@function:
//		CQueryContext::CQueryContext
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CQueryContext::CQueryContext(duckdb::unique_ptr<Operator> pexpr, CReqdPropPlan* prpp, duckdb::vector<ColumnBinding> colref_array, duckdb::vector<std::string> pdrgpmdname, bool fDeriveStats)
	: m_prpp(prpp), m_pdrgpcr(colref_array), m_fDeriveStats(fDeriveStats)
{
	duckdb::vector<ColumnBinding> pcrsOutputAndOrderingCols;
	duckdb::vector<ColumnBinding> pcrsOrderSpec = prpp->m_peo->m_pos->PcrsUsed();
	pcrsOutputAndOrderingCols.insert(pcrsOutputAndOrderingCols.end(), colref_array.begin(), colref_array.end());
	pcrsOutputAndOrderingCols.insert(pcrsOutputAndOrderingCols.end(), pcrsOrderSpec.begin(), pcrsOrderSpec.end());
	for(auto &child : pdrgpmdname)
	{
		m_pdrgpmdname.push_back(child);
	}
	/* I comment here */
	// m_pexpr = CExpressionPreprocessor::PexprPreprocess(pexpr, pcrsOutputAndOrderingCols);
	m_pexpr = std::move(pexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CQueryContext::~CQueryContext
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CQueryContext::~CQueryContext()
{
	// m_pexpr->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CQueryContext::PopTop
//
//	@doc:
// 		 Return top level operator in the given expression
//
//---------------------------------------------------------------------------
LogicalOperator* CQueryContext::PopTop(LogicalOperator* pexpr)
{
	// skip CTE anchors if any
	LogicalOperator* pexprCurr = pexpr;
	while (LogicalOperatorType::LOGICAL_CTE_REF == pexprCurr->logical_type)
	{
		pexprCurr = (LogicalOperator*)pexprCurr->children[0].get();
	}
	return pexprCurr;
}

//---------------------------------------------------------------------------
//	@function:
//		CQueryContext::PqcGenerate
//
//	@doc:
// 		Generate the query context for the given expression and array of
//		output column ref ids
//
//---------------------------------------------------------------------------
CQueryContext* CQueryContext::PqcGenerate(duckdb::unique_ptr<Operator> pexpr, duckdb::vector<ULONG*> pdrgpulQueryOutputColRefId, duckdb::vector<std::string> pdrgpmdname, bool fDeriveStats)
{
	duckdb::vector<ColumnBinding> pcrs;
	duckdb::vector<ColumnBinding> colref_array;
	COptCtxt* poptctxt = COptCtxt::PoctxtFromTLS();
	// Collect required properties (prpp) at the top level:
	COrderSpec* pos = new COrderSpec();
	// Ensure order, distribution and rewindability meet 'satisfy' matching at the top level
	CEnfdOrder* peo = new CEnfdOrder(pos, CEnfdOrder::EomSatisfy);
	CReqdPropPlan* prpp = new CReqdPropPlan(pcrs, peo);
	// Finally, create the CQueryContext
	return new CQueryContext(std::move(pexpr), prpp, colref_array, pdrgpmdname, fDeriveStats);
}