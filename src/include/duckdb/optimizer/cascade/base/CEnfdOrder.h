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
#include "duckdb/optimizer/cascade/base/CEnfdProp.h"
#include "duckdb/optimizer/cascade/base/COrderSpec.h"

namespace gpopt
{
using namespace gpos;

// prototypes
class CPhysical;

//---------------------------------------------------------------------------
//	@class:
//		CEnfdOrder
//
//	@doc:
//		Enforceable order property;
//
//---------------------------------------------------------------------------
class CEnfdOrder : public CEnfdProp
{
public:
	// type of order matching function
	enum EOrderMatching
	{
		EomSatisfy = 0,

		EomSentinel
	};

private:
	// required sort order
	COrderSpec *m_pos;

	// order matching type
	EOrderMatching m_eom;

	// private copy ctor
	CEnfdOrder(const CEnfdOrder &);

	// names of order matching types
	static const CHAR *m_szOrderMatching[EomSentinel];

public:
	// ctor
	CEnfdOrder(COrderSpec *pos, EOrderMatching eom);

	// dtor
	virtual ~CEnfdOrder();

	// hash function
	virtual ULONG HashValue() const;

	// check if the given order specification is compatible with the
	// order specification of this object for the specified matching type
	BOOL FCompatible(COrderSpec *pos) const;

	// required order accessor
	COrderSpec *
	PosRequired() const
	{
		return m_pos;
	}

	// get order enforcing type for the given operator
	EPropEnforcingType Epet(CExpressionHandle &exprhdl, CPhysical *popPhysical,
							BOOL fOrderReqd) const;

	// property spec accessor
	virtual CPropSpec *
	Pps() const
	{
		return m_pos;
	}

	// return matching type
	EOrderMatching
	Eom() const
	{
		return m_eom;
	}

	// matching function
	BOOL
	Matches(CEnfdOrder *peo)
	{
		GPOS_ASSERT(NULL != peo);

		return m_eom == peo->Eom() && m_pos->Matches(peo->PosRequired());
	}

	// print function
	virtual IOstream &OsPrint(IOstream &os) const;

};	// class CEnfdOrder

}  // namespace gpopt

#endif
