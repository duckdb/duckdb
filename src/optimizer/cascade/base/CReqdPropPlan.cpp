//---------------------------------------------------------------------------
//	@filename:
//		CReqdPropPlan.cpp
//
//	@doc:
//		Required plan properties;
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CReqdPropPlan.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CPrintablePointer.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropPlan.h"

namespace gpopt
{
using namespace duckdb;

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::CReqdPropPlan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CReqdPropPlan::CReqdPropPlan(duckdb::vector<ColumnBinding> pcrs, CEnfdOrder* peo)
	: m_pcrs(pcrs), m_peo(peo)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::~CReqdPropPlan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CReqdPropPlan::~CReqdPropPlan()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::ComputeReqdCols
//
//	@doc:
//		Compute required columns
//
//---------------------------------------------------------------------------
void CReqdPropPlan::ComputeReqdCols(CExpressionHandle &exprhdl, CReqdProp* prpInput, ULONG child_index, duckdb::vector<CDrvdProp*> pdrgpdpCtxt)
{
	CReqdPropPlan* prppInput = CReqdPropPlan::Prpp(prpInput);
	PhysicalOperator* popPhysical = (PhysicalOperator*)exprhdl.Pop();
	m_pcrs = popPhysical->PcrsRequired(exprhdl, prppInput->m_pcrs, child_index, pdrgpdpCtxt, 0);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::Compute
//
//	@doc:
//		Compute required props
//
//---------------------------------------------------------------------------
void CReqdPropPlan::Compute(CExpressionHandle &exprhdl, CReqdProp* prpInput, ULONG child_index, duckdb::vector<CDrvdProp*> pdrgpdpCtxt, ULONG ulOptReq)
{
	CReqdPropPlan* prppInput = CReqdPropPlan::Prpp(prpInput);
	PhysicalOperator* popPhysical = (PhysicalOperator*)exprhdl.Pop();
	ComputeReqdCols(exprhdl, prpInput, child_index, pdrgpdpCtxt);
	m_peo = new CEnfdOrder(popPhysical->PosRequired(exprhdl, prppInput->m_peo->m_pos, child_index, pdrgpdpCtxt, ulOptReq), popPhysical->Eom(prppInput, child_index, pdrgpdpCtxt, ulOptReq));
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::Equals
//
//	@doc:
//		Check if expression attached to handle provides required columns
//		by all plan properties
//
//---------------------------------------------------------------------------
bool CReqdPropPlan::FProvidesReqdCols(CExpressionHandle &exprhdl, ULONG ulOptReq) const
{
	// check if operator provides required columns
	if (!((PhysicalOperator*)exprhdl.Pop())->FProvidesReqdCols(exprhdl, m_pcrs, ulOptReq))
	{
		return false;
	}
	duckdb::vector<ColumnBinding> pcrsOutput = exprhdl.DeriveOutputColumns();
	// check if property spec members use columns from operator output
	bool fProvidesReqdCols = true;
	COrderSpec* pps = m_peo->m_pos;
	if (NULL == pps)
	{
		return fProvidesReqdCols;
	}
	duckdb::vector<ColumnBinding> pcrsUsed = pps->PcrsUsed();
	duckdb::vector<ColumnBinding> v;
	for(auto &child : pcrsUsed) {
		fProvidesReqdCols = false;
		for(auto &sub_child : pcrsOutput) {
			if(child == sub_child) {
				fProvidesReqdCols = true;
				break;
			}
		}
		if(!fProvidesReqdCols) {
			return fProvidesReqdCols;
		}
	}
	return fProvidesReqdCols;
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
bool CReqdPropPlan::Equals(CReqdPropPlan* prpp) const
{
	return CUtils::Equals(m_pcrs, prpp->m_pcrs) && m_peo->Matches(prpp->m_peo);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::HashValue
//
//	@doc:
//		Compute hash value using required columns and required sort order
//
//---------------------------------------------------------------------------
ULONG CReqdPropPlan::HashValue() const
{
	ULONG ulHash = 0;
	for(ULONG m = 0; m < m_pcrs.size(); m++)
	{
		ulHash = gpos::CombineHashes(ulHash, gpos::HashValue(&m_pcrs[m]));
	}
	ulHash = gpos::CombineHashes(ulHash, m_peo->HashValue());
	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::FSatisfied
//
//	@doc:
//		Check if plan properties are satisfied by the given derived properties
//
//---------------------------------------------------------------------------
bool CReqdPropPlan::FSatisfied(CDrvdPropRelational* pdprel, CDrvdPropPlan* pdpplan) const
{
	// first, check satisfiability of relational properties
	if (!pdprel->FSatisfies(this))
	{
		return false;
	}
	// otherwise, check satisfiability of all plan properties
	return pdpplan->FSatisfies(this);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::FCompatible
//
//	@doc:
//		Check if plan properties are compatible with the given derived properties
//
//---------------------------------------------------------------------------
bool CReqdPropPlan::FCompatible(CExpressionHandle &exprhdl, PhysicalOperator* popPhysical, CDrvdPropRelational* pdprel, CDrvdPropPlan* pdpplan) const
{
	// first, check satisfiability of relational properties, including required columns
	if (!pdprel->FSatisfies(this))
	{
		return false;
	}
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::PrppEmpty
//
//	@doc:
//		Generate empty required properties
//
//---------------------------------------------------------------------------
CReqdPropPlan* CReqdPropPlan::PrppEmpty()
{
	duckdb::vector<ColumnBinding> pcrs;
	COrderSpec* pos = new COrderSpec();
	CEnfdOrder* peo = new CEnfdOrder(pos, CEnfdOrder::EomSatisfy);
	return new CReqdPropPlan(pcrs, peo);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::UlHashForCostBounding
//
//	@doc:
//		Hash function used for cost bounding
//
//---------------------------------------------------------------------------
ULONG CReqdPropPlan::UlHashForCostBounding(CReqdPropPlan* prpp)
{
	duckdb::vector<ColumnBinding> v = prpp->m_pcrs;
	ULONG ulHash = 0;
	for(size_t m = 0; m < v.size(); m++)
	{
		ulHash = gpos::CombineHashes(ulHash, gpos::HashValue(&v[m]));
	}
	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::FEqualForCostBounding
//
//	@doc:
//		Equality function used for cost bounding
//
//---------------------------------------------------------------------------
bool CReqdPropPlan::FEqualForCostBounding(CReqdPropPlan* prppFst, CReqdPropPlan* prppSnd)
{
	return CUtils::Equals(prppFst->m_pcrs, prppSnd->m_pcrs);
}
}