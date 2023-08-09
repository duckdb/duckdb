//---------------------------------------------------------------------------
//	@filename:
//		CEnfdOrder.h
//
//	@doc:
//		Enforceable order property
//---------------------------------------------------------------------------
#ifndef GPOPT_CEnfdOrder_H
#define GPOPT_CEnfdOrder_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/COrderSpec.h"

namespace duckdb
{
	class PhysicalOperator;
}

namespace gpopt
{
using namespace gpos;
using namespace duckdb;

//---------------------------------------------------------------------------
//	@class:
//		CEnfdOrder
//
//	@doc:
//		Enforceable order property;
//
//---------------------------------------------------------------------------
class CEnfdOrder
{
public:
	enum EPropEnforcingType
	{ EpetRequired, EpetOptional, EpetProhibited, EpetUnnecessary, EpetSentinel };

public:
	// type of order matching function
	enum EOrderMatching
	{ EomSatisfy = 0, EomSentinel };

public:
	// required sort order
	COrderSpec* m_pos;

	// order matching type
	EOrderMatching m_eom;

	// names of order matching types
	static const CHAR* m_szOrderMatching[EomSentinel];

public:
	// ctor
	CEnfdOrder(COrderSpec* pos, EOrderMatching eom);

	// no copy ctor
	CEnfdOrder(const CEnfdOrder &) = delete;

	// dtor
	virtual ~CEnfdOrder();

	// hash function
	virtual ULONG HashValue() const;

	// check if the given order specification is compatible with the
	// order specification of this object for the specified matching type
	bool FCompatible(COrderSpec* pos) const;

	// get order enforcing type for the given operator
	EPropEnforcingType Epet(CExpressionHandle &exprhdl, PhysicalOperator* popPhysical, bool fOrderReqd) const;

	// check if operator requires an enforcer under given enforceable property
	// based on the derived enforcing type
	static bool FEnforce(EPropEnforcingType epet)
	{
		return CEnfdOrder::EpetOptional == epet || CEnfdOrder::EpetRequired == epet;
	}

	// append enforcers to dynamic array for the given plan properties
	void AppendEnforcers(CReqdPropPlan* prpp, duckdb::vector<duckdb::unique_ptr<Operator>> &pdrgpexpr, duckdb::unique_ptr<Operator> pexprChild, CEnfdOrder::EPropEnforcingType epet, CExpressionHandle &exprhdl)
	{
		if (FEnforce(epet))
		{
			m_pos->AppendEnforcers(exprhdl, prpp, pdrgpexpr, std::move(pexprChild));
		}
	}

	// matching function
	bool Matches(CEnfdOrder *peo)
	{
		return m_eom == peo->m_eom && m_pos->Matches(peo->m_pos);
	}

	// check if operator requires optimization under given enforceable property
	// based on the derived enforcing type
	static bool FOptimize(EPropEnforcingType epet)
	{
		return CEnfdOrder::EpetOptional == epet || CEnfdOrder::EpetUnnecessary == epet;
	}
};	// class CEnfdOrder
}  // namespace gpopt
#endif