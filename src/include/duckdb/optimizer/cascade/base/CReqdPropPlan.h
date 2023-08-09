//---------------------------------------------------------------------------
//	@filename:
//		CReqdPropPlan.h
//
//	@doc:
//		Derived required relational properties
//---------------------------------------------------------------------------
#ifndef GPOPT_CReqdPropPlan_H
#define GPOPT_CReqdPropPlan_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/planner/expression.hpp"
#include "duckdb/optimizer/cascade/base/CReqdProp.h"
#include "duckdb/optimizer/cascade/base/CEnfdOrder.h"

namespace gpopt
{
using namespace duckdb;
using namespace gpos;

// forward declaration
class CDrvdPropRelational;
class CDrvdPropPlan;
class CEnfdOrder;
class CExpressionHandle;

//---------------------------------------------------------------------------
//	@class:
//		CReqdPropPlan
//
//	@doc:
//		Required plan properties container.
//
//---------------------------------------------------------------------------
class CReqdPropPlan : public CReqdProp
{
public:
	// required columns
	duckdb::vector<ColumnBinding> m_pcrs;

	// required sort order
	CEnfdOrder* m_peo;

public:
	// default ctor
	CReqdPropPlan()
		: m_peo(NULL)
	{
	}

	// ctor
	CReqdPropPlan(duckdb::vector<ColumnBinding> pcrs, CEnfdOrder* peo);
	
	// copy ctor
	CReqdPropPlan(const CReqdPropPlan & other) = delete;
	
	// dtor
	virtual ~CReqdPropPlan();

	// type of properties
	virtual bool FPlan() const override
	{
		return true;
	}

	// required properties computation function
	virtual void Compute(CExpressionHandle &exprhdl, CReqdProp* prpInput, ULONG child_index, duckdb::vector<CDrvdProp*> pdrgpdpCtxt, ULONG ulOptReq) override;

	// required columns computation function
	void ComputeReqdCols(CExpressionHandle &exprhdl, CReqdProp* prpInput, ULONG child_index, duckdb::vector<CDrvdProp*> pdrgpdpCtxt);
	
	// equality function
	bool Equals(CReqdPropPlan* prpp) const;

	// hash function
	ULONG HashValue() const;

	// check if plan properties are satisfied by the given derived properties
	bool FSatisfied(CDrvdPropRelational* pdprel, CDrvdPropPlan* pdpplan) const;

	// check if plan properties are compatible with the given derived properties
	bool FCompatible(CExpressionHandle &exprhdl, PhysicalOperator* popPhysical, CDrvdPropRelational* pdprel, CDrvdPropPlan* pdpplan) const;

	// check if expression attached to handle provides required columns by all plan properties
	bool FProvidesReqdCols(CExpressionHandle &exprhdl, ULONG ulOptReq) const;

	// shorthand for conversion
	static CReqdPropPlan* Prpp(CReqdProp* prp)
	{
		return (CReqdPropPlan*)prp;
	}

	//generate empty required properties
	static CReqdPropPlan* PrppEmpty();

	// hash function used for cost bounding
	static ULONG UlHashForCostBounding(CReqdPropPlan* prpp);

	// equality function used for cost bounding
	static bool FEqualForCostBounding(CReqdPropPlan* prppFst, CReqdPropPlan* prppSnd);
	// map input required and derived plan properties into new required plan properties
	// static CReqdPropPlan* PrppRemapForCTE(CReqdPropPlan *prppInput, CDrvdPropPlan *pdpplanInput);
};	// class CReqdPropPlan
}  // namespace gpopt
#endif