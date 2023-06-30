//---------------------------------------------------------------------------
//	@filename:
//		CDrvdPropCtxtPlan.h
//
//	@doc:
//		Container of information passed among expression nodes during
//		derivation of plan properties
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CDrvdPropCtxtPlan_H
#define GPOPT_CDrvdPropCtxtPlan_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CCTEMap.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxt.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CDrvdPropCtxtPlan
//
//	@doc:
//		Container of information passed among expression nodes during
//		derivation of plan properties
//
//---------------------------------------------------------------------------
class CDrvdPropCtxtPlan : public CDrvdPropCtxt
{
private:
	// map of CTE id to producer plan properties
	UlongToDrvdPropPlanMap *m_phmulpdpCTEs;

	// the number of expected partition selectors
	ULONG m_ulExpectedPartitionSelectors;

	// if true, a call to AddProps updates the CTE.
	BOOL m_fUpdateCTEMap;

	// private copy ctor
	CDrvdPropCtxtPlan(const CDrvdPropCtxtPlan &);

protected:
	// copy function
	virtual CDrvdPropCtxt *PdpctxtCopy(CMemoryPool *mp) const;

	// add props to context
	virtual void AddProps(CDrvdProp *pdp);

public:
	// ctor
	CDrvdPropCtxtPlan(CMemoryPool *mp, BOOL fUpdateCTEMap = true);

	// dtor
	virtual ~CDrvdPropCtxtPlan();

	ULONG
	UlExpectedPartitionSelectors() const
	{
		return m_ulExpectedPartitionSelectors;
	}

	// set the number of expected partition selectors based on the given
	// operator and the given cost context
	void SetExpectedPartitionSelectors(COperator *pop, CCostContext *pcc);

	// print
	virtual IOstream &OsPrint(IOstream &os) const;

	// return the plan properties of CTE producer with given id
	CDrvdPropPlan *PdpplanCTEProducer(ULONG ulCTEId) const;

	// copy plan properties of given CTE prdoucer
	void CopyCTEProducerProps(CDrvdPropPlan *pdpplan, ULONG ulCTEId);

#ifdef GPOS_DEBUG

	// is it a plan property context?
	virtual BOOL
	FPlan() const
	{
		return true;
	}

#endif	// GPOS_DEBUG

	// conversion function
	static CDrvdPropCtxtPlan *
	PdpctxtplanConvert(CDrvdPropCtxt *pdpctxt)
	{
		GPOS_ASSERT(NULL != pdpctxt);

		return reinterpret_cast<CDrvdPropCtxtPlan *>(pdpctxt);
	}

};	// class CDrvdPropCtxtPlan

}  // namespace gpopt

#endif