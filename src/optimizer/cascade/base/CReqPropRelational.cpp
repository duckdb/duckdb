//---------------------------------------------------------------------------
//	@filename:
//		CReqPropRelational.cpp
//
//	@doc:
//		Required relational properties;
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CReqdPropRelational.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"

#include "duckdb/optimizer/cascade/engine/CEngine.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropRelational::CReqdPropRelational
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CReqdPropRelational::CReqdPropRelational()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropRelational::CReqdPropRelational
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CReqdPropRelational::CReqdPropRelational(duckdb::vector<ColumnBinding> pcrs)
{
	for(auto &child : pcrs)
	{
		m_pcrsStat.emplace_back(child.table_index, child.column_index);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropRelational::~CReqdPropRelational
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CReqdPropRelational::~CReqdPropRelational()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropRelational::Compute
//
//	@doc:
//		Compute required props
//
//---------------------------------------------------------------------------
void CReqdPropRelational::Compute(CExpressionHandle &exprhdl, CReqdProp* prpInput, ULONG child_index, duckdb::vector<CDrvdProp*> pdrgpdpCtxt, ULONG ulOptReq)
{
	CReqdPropRelational* prprelInput = CReqdPropRelational::GetReqdRelationalProps(prpInput);
	m_pcrsStat = ((LogicalOperator*)exprhdl.Pop())->GetColumnBindings();
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropRelational::GetReqdRelationalProps
//
//	@doc:
//		Short hand for conversion
//
//---------------------------------------------------------------------------
CReqdPropRelational* CReqdPropRelational::GetReqdRelationalProps(CReqdProp* prp)
{
	return (CReqdPropRelational*)prp;
}


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropRelational::PrprelDifference
//
//	@doc:
//		Return difference from given properties
//
//---------------------------------------------------------------------------
CReqdPropRelational* CReqdPropRelational::PrprelDifference(CReqdPropRelational* prprel)
{
	duckdb::vector<ColumnBinding> pcrs;
	duckdb::vector<ColumnBinding> v2 = prprel->PcrsStat();
	std::set_difference(m_pcrsStat.begin(), m_pcrsStat.end(), v2.begin(), v2.end(), pcrs.begin());
	return new CReqdPropRelational(pcrs);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropRelational::IsEmpty
//
//	@doc:
//		Return true if property container is empty
//
//---------------------------------------------------------------------------
bool CReqdPropRelational::IsEmpty() const
{
	return m_pcrsStat.size() == 0;
}