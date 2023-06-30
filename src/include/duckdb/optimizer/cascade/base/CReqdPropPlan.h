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

#include "duckdb/optimizer/cascade/base/CColRef.h"
#include "duckdb/optimizer/cascade/base/CReqdProp.h"

namespace gpopt
{
using namespace gpos;

// forward declaration
class CColRefSet;
class CDrvdPropRelational;
class CDrvdPropPlan;
class CEnfdOrder;
class CEnfdDistribution;
class CEnfdRewindability;
class CEnfdPartitionPropagation;
class CExpressionHandle;
class CCTEReq;
class CPartInfo;
class CPartFilterMap;
class CPhysical;
class CPropSpec;

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
private:
	// required columns
	CColRefSet* m_pcrs;

	// required sort order
	CEnfdOrder* m_peo;

	// required ctes
	CCTEReq* m_pcter;

	// private copy ctor
	CReqdPropPlan(const CReqdPropPlan &);

	// combine derived part filter map from input requirements and
	// derived plan properties in the passed context
	CPartFilterMap* PpfmCombineDerived(CMemoryPool* mp, CExpressionHandle &exprhdl, CReqdPropPlan* prppInput, ULONG child_index, CDrvdPropArray* pdrgpdpCtxt);

public:
	// default ctor
	CReqdPropPlan()
		: m_pcrs(NULL), m_peo(NULL), m_pcter(NULL)
	{
	}

	// ctor
	CReqdPropPlan(CColRefSet* pcrs, CEnfdOrder* peo, CCTEReq* pcter);

	// dtor
	virtual ~CReqdPropPlan();

	// type of properties
	virtual BOOL FPlan() const
	{
		GPOS_ASSERT(!FRelational());
		return true;
	}

	// required properties computation function
	virtual void Compute(CMemoryPool *mp, CExpressionHandle &exprhdl, CReqdProp *prpInput, ULONG child_index, CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq);

	// required columns computation function
	void ComputeReqdCols(CMemoryPool *mp, CExpressionHandle &exprhdl, CReqdProp *prpInput, ULONG child_index, CDrvdPropArray *pdrgpdpCtxt);
	void ComputeReqdCols(CMemoryPool* mp, PhysicalOperator* expr, CReqdProp* prpInput, ULONG child_index, CDrvdPropArray* pdrgpdpCtxt);
	
	// required ctes computation function
	void ComputeReqdCTEs(CMemoryPool *mp, CExpressionHandle &exprhdl, CReqdProp *prpInput, ULONG child_index, CDrvdPropArray *pdrgpdpCtxt);

	// required columns accessor
	CColRefSet* PcrsRequired() const
	{
		return m_pcrs;
	}

	// required order accessor
	CEnfdOrder* Peo() const
	{
		return m_peo;
	}

	// required cte accessor
	CCTEReq* Pcter() const
	{
		return m_pcter;
	}

	// given a property spec type, return the corresponding property spec member
	CPropSpec *Pps(ULONG ul) const;

	// equality function
	BOOL Equals(const CReqdPropPlan *prpp) const;

	// hash function
	ULONG HashValue() const;

	// check if plan properties are satisfied by the given derived properties
	BOOL FSatisfied(const CDrvdPropRelational *pdprel, const CDrvdPropPlan *pdpplan) const;

	// check if plan properties are compatible with the given derived properties
	BOOL FCompatible(CExpressionHandle &exprhdl, CPhysical *popPhysical, const CDrvdPropRelational *pdprel, const CDrvdPropPlan *pdpplan) const;

	// initialize partition propagation requirements
	void InitReqdPartitionPropagation(CMemoryPool *mp, CPartInfo *ppartinfo);

	// check if expression attached to handle provides required columns by all plan properties
	BOOL FProvidesReqdCols(CMemoryPool *mp, CExpressionHandle &exprhdl, ULONG ulOptReq) const;

	// shorthand for conversion
	static CReqdPropPlan* Prpp(CReqdProp *prp)
	{
		GPOS_ASSERT(NULL != prp);
		return dynamic_cast<CReqdPropPlan*>(prp);
	}

	//generate empty required properties
	static CReqdPropPlan *PrppEmpty(CMemoryPool *mp);

	// hash function used for cost bounding
	static ULONG UlHashForCostBounding(const CReqdPropPlan *prpp);

	// equality function used for cost bounding
	static BOOL FEqualForCostBounding(const CReqdPropPlan *prppFst,
									  const CReqdPropPlan *prppSnd);

	// map input required and derived plan properties into new required plan properties
	static CReqdPropPlan *PrppRemapForCTE(CMemoryPool *mp, CReqdPropPlan *prppInput, CDrvdPropPlan *pdpplanInput, UlongToColRefMap *colref_mapping);

	// print function
	virtual IOstream &OsPrint(IOstream &os) const;

};	// class CReqdPropPlan

}  // namespace gpopt

#endif
