//---------------------------------------------------------------------------
//	@filename:
//		COrderSpec.cpp
//
//	@doc:
//		Specification of order property
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/COrderSpec.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/optimizer/cascade/base/CReqdPropPlan.h"

using namespace gpopt;
using namespace duckdb;

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::COrderSpec
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
COrderSpec::COrderSpec()
{
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::~COrderSpec
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
COrderSpec::~COrderSpec()
{
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::Append
//
//	@doc:
//		Append order expression;
//
//---------------------------------------------------------------------------
void COrderSpec::Append(OrderType type, OrderByNullType null_order, Expression* expr)
{
	m_pdrgpoe.emplace_back(type, null_order, duckdb::unique_ptr<Expression>(expr));
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::Matches
//
//	@doc:
//		Check for equality between order specs
//
//---------------------------------------------------------------------------
bool COrderSpec::Matches(COrderSpec* pos) const
{
	bool fMatch = m_pdrgpoe.size() == pos->m_pdrgpoe.size() && FSatisfies(pos);
	return fMatch;
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::FSatisfies
//
//	@doc:
//		Check if this order spec satisfies the given one
//
//---------------------------------------------------------------------------
bool COrderSpec::FSatisfies(COrderSpec* pos) const
{
	const ULONG arity = pos->m_pdrgpoe.size();
	bool fSatisfies = (m_pdrgpoe.size() >= arity);
	for (ULONG ul = 0; fSatisfies && ul < arity; ul++)
	{
		fSatisfies = m_pdrgpoe[ul].Equals(pos->m_pdrgpoe[ul]);
	}
	return fSatisfies;
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::AppendEnforcers
//
//	@doc:
//		Add required enforcers enforcers to dynamic array
//
//---------------------------------------------------------------------------
void COrderSpec::AppendEnforcers(CExpressionHandle &exprhdl, CReqdPropPlan* prpp,
								duckdb::vector<duckdb::unique_ptr<Operator>> &pdrgpexpr,
								duckdb::unique_ptr<Operator> pexpr)
{
	duckdb::vector<idx_t> projections;
	for (idx_t i = 0; i < pexpr->types.size(); i++)
	{
		projections.push_back(i);
	}
	duckdb::vector<BoundOrderByNode> v_orders;
	for(auto &child : prpp->m_peo->m_pos->m_pdrgpoe) {
		v_orders.emplace_back(child.Copy());
	}
	auto pexprSort = make_uniq<PhysicalOrder>(pexpr->types, std::move(v_orders), std::move(projections), 0);
	pexprSort->AddChild(std::move(pexpr));
	pdrgpexpr.push_back(std::move(pexprSort));
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::HashValue
//
//	@doc:
//		Hash of components
//
//---------------------------------------------------------------------------
ULONG COrderSpec::HashValue() const
{
	ULONG ulHash = 0;
	ULONG arity = m_pdrgpoe.size();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		auto& poe = m_pdrgpoe[ul];
		ulHash = gpos::CombineHashes(ulHash, gpos::HashPtr<BoundOrderByNode>(&poe));
	}
	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::PosExcludeColumns
//
//	@doc:
//		Return a copy of the order spec after excluding the given columns
//
//---------------------------------------------------------------------------
COrderSpec* COrderSpec::PosExcludeColumns(duckdb::vector<ColumnBinding> pcrs)
{
	COrderSpec* pos = new COrderSpec();
	const ULONG num_cols = m_pdrgpoe.size();
	for (ULONG ul = 0; ul < num_cols; ul++)
	{
		auto& poe = m_pdrgpoe[ul];
		ColumnBinding colref = ((BoundColumnRefExpression*)poe.expression.get())->binding;
		if (std::find(pcrs.begin(), pcrs.end(), colref) != pcrs.end())
		{
			continue;
		}
		pos->Append(poe.type, poe.null_order, poe.expression.get());
	}
	return pos;
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::ExtractCols
//
//	@doc:
//		Extract columns from order spec into the given column set
//
//---------------------------------------------------------------------------
void COrderSpec::ExtractCols(duckdb::vector<ColumnBinding> pcrs) const
{
	const ULONG ulOrderExprs = m_pdrgpoe.size();
	for (ULONG ul = 0; ul < ulOrderExprs; ul++)
	{
		ColumnBinding cell = ((BoundColumnRefExpression*)m_pdrgpoe[ul].expression.get())->binding;
		pcrs.emplace_back(cell.table_index, cell.column_index);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::PcrsUsed
//
//	@doc:
//		Extract colref set from order components
//
//---------------------------------------------------------------------------
duckdb::vector<ColumnBinding> COrderSpec::PcrsUsed() const
{
	duckdb::vector<ColumnBinding> pcrs;
	ExtractCols(pcrs);
	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::GetColRefSet
//
//	@doc:
//		Extract colref set from order specs in the given array
//
//---------------------------------------------------------------------------
duckdb::vector<ColumnBinding> COrderSpec::GetColRefSet(duckdb::vector<COrderSpec*> pdrgpos)
{
	duckdb::vector<ColumnBinding> pcrs;
	const ULONG ulOrderSpecs = pdrgpos.size();
	for (ULONG ulSpec = 0; ulSpec < ulOrderSpecs; ulSpec++)
	{
		COrderSpec* pos = pdrgpos[ulSpec];
		pos->ExtractCols(pcrs);
	}
	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::PdrgposExclude
//
//	@doc:
//		Filter out array of order specs from order expressions using the
//		passed columns
//
//---------------------------------------------------------------------------
duckdb::vector<COrderSpec*> COrderSpec::PdrgposExclude(duckdb::vector<COrderSpec*> pdrgpos, duckdb::vector<ColumnBinding> pcrsToExclude)
{
	if (0 == pcrsToExclude.size())
	{
		// no columns to exclude
		return pdrgpos;
	}
	duckdb::vector<COrderSpec*> pdrgposNew;
	const ULONG ulOrderSpecs = pdrgpos.size();
	for (ULONG ulSpec = 0; ulSpec < ulOrderSpecs; ulSpec++)
	{
		COrderSpec* pos = pdrgpos[ulSpec];
		COrderSpec* posNew = pos->PosExcludeColumns(pcrsToExclude);
		pdrgposNew.push_back(posNew);
	}
	return pdrgposNew;
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::Equals
//
//	@doc:
//		 Matching function over order spec arrays
//
//---------------------------------------------------------------------------
bool COrderSpec::Equals(duckdb::vector<COrderSpec*> pdrgposFirst, duckdb::vector<COrderSpec*> pdrgposSecond)
{
	if (0 == pdrgposFirst.size() || 0 == pdrgposFirst.size())
	{
		return (0 == pdrgposFirst.size() && 0 == pdrgposFirst.size());
	}
	if (pdrgposFirst.size() != pdrgposSecond.size())
	{
		return false;
	}
	const ULONG size = pdrgposFirst.size();
	bool fMatch = true;
	for (ULONG ul = 0; fMatch && ul < size; ul++)
	{
		fMatch = pdrgposFirst[ul]->Matches(pdrgposSecond[ul]);
	}
	return fMatch;
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::HashValue
//
//	@doc:
//		 Combine hash values of a maximum number of entries
//
//---------------------------------------------------------------------------
ULONG COrderSpec::HashValue(const duckdb::vector<COrderSpec*> pdrgpos, ULONG ulMaxSize)
{
	ULONG size = std::min(ulMaxSize, (ULONG)pdrgpos.size());
	ULONG ulHash = 0;
	for (ULONG ul = 0; ul < size; ul++)
	{
		ulHash = gpos::CombineHashes(ulHash, pdrgpos[ul]->HashValue());
	}
	return ulHash;
}