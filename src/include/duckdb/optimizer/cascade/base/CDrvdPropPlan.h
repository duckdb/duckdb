//---------------------------------------------------------------------------
//	@filename:
//		CDrvdPropPlan.h
//
//	@doc:
//		Derived physical properties
//---------------------------------------------------------------------------
#ifndef GPOPT_CDrvdPropPlan_H
#define GPOPT_CDrvdPropPlan_H

#include "duckdb/optimizer/cascade/base/CDrvdProp.h"
#include "duckdb/optimizer/cascade/operators/Operator.h"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"

namespace gpopt
{
using namespace duckdb;
using namespace gpos;

// fwd declaration
class COrderSpec;
class CReqdPropPlan;
class CDrvdPropPlan;

//---------------------------------------------------------------------------
//	@class:
//		CDrvdPropPlan
//
//	@doc:
//		Derived plan properties container.
//
//		These are properties that are expression-specific and they depend on
//		the physical implementation. This includes sort order, distribution,
//		rewindability, partition propagation spec and CTE map.
//
//---------------------------------------------------------------------------
class CDrvdPropPlan : public CDrvdProp
{
public:
	// derived sort order
	COrderSpec* m_pos;

	// derived cte map
	// CCTEMap* m_pcm;

	// copy CTE producer plan properties from given context to current object
	// void CopyCTEProducerPlanProps(CDrvdPropCtxt* pdpctxt, Operator* pop);

public:
	// ctor
	CDrvdPropPlan();

	CDrvdPropPlan(const CDrvdPropPlan &) = delete;

	// dtor
	virtual ~CDrvdPropPlan();

	// type of properties
	CDrvdProp::EPropType Ept() override
	{
		return CDrvdProp::EPropType::EptPlan;
	}

	// derivation function
	void Derive(gpopt::CExpressionHandle& pop, CDrvdPropCtxt* pdpctxt) override;

	// short hand for conversion
	static CDrvdPropPlan* Pdpplan(CDrvdProp* pdp);

	// cte map
	// CCTEMap* GetCostModel() const
	// {
	//	return m_pcm;
	// }

	// hash function
	virtual ULONG HashValue() const;

	// equality function
	virtual ULONG Equals(const CDrvdPropPlan* pdpplan) const;

	// check for satisfying required plan properties
	virtual BOOL FSatisfies(const CReqdPropPlan* prpp) const override;
};	// class CDrvdPropPlan
}  // namespace gpopt
#endif