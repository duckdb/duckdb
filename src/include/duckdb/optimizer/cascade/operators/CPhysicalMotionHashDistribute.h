//---------------------------------------------------------------------------
//	@filename:
//		CPhysicalMotionHashDistribute.h
//
//	@doc:
//		Physical Hash distribute motion operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalMotionHashDistribute_H
#define GPOPT_CPhysicalMotionHashDistribute_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpecHashed.h"
#include "duckdb/optimizer/cascade/base/COrderSpec.h"
#include "duckdb/optimizer/cascade/operators/CPhysicalMotion.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalMotionHashDistribute
//
//	@doc:
//		Hash distribute motion operator
//
//---------------------------------------------------------------------------
class CPhysicalMotionHashDistribute : public CPhysicalMotion
{
private:
	// hash distribution spec
	CDistributionSpecHashed *m_pdsHashed;

	// required columns in distribution spec
	CColRefSet *m_pcrsRequiredLocal;

	// private copy ctor
	CPhysicalMotionHashDistribute(const CPhysicalMotionHashDistribute &);

public:
	// ctor
	CPhysicalMotionHashDistribute(CMemoryPool *mp,
								  CDistributionSpecHashed *pdsHashed);

	// dtor
	virtual ~CPhysicalMotionHashDistribute();

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopPhysicalMotionHashDistribute;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CPhysicalMotionHashDistribute";
	}

	// output distribution accessor
	virtual CDistributionSpec *
	Pds() const
	{
		return m_pdsHashed;
	}

	// is motion eliminating duplicates
	BOOL
	IsDuplicateSensitive() const
	{
		return m_pdsHashed->IsDuplicateSensitive();
	}

	// match function
	virtual BOOL Matches(COperator *) const;

	//-------------------------------------------------------------------------------------
	// Required Plan Properties
	//-------------------------------------------------------------------------------------

	// compute required output columns of the n-th child
	virtual CColRefSet *PcrsRequired(CMemoryPool *mp,
									 CExpressionHandle &exprhdl,
									 CColRefSet *pcrsInput, ULONG child_index,
									 CDrvdPropArray *pdrgpdpCtxt,
									 ULONG ulOptReq);

	// compute required sort order of the n-th child
	virtual COrderSpec *PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
									COrderSpec *posInput, ULONG child_index,
									CDrvdPropArray *pdrgpdpCtxt,
									ULONG ulOptReq) const;

	// check if required columns are included in output columns
	virtual BOOL FProvidesReqdCols(CExpressionHandle &exprhdl,
								   CColRefSet *pcrsRequired,
								   ULONG ulOptReq) const;

	//-------------------------------------------------------------------------------------
	// Derived Plan Properties
	//-------------------------------------------------------------------------------------

	// derive sort order
	virtual COrderSpec *PosDerive(CMemoryPool *mp,
								  CExpressionHandle &exprhdl) const;

	//-------------------------------------------------------------------------------------
	// Enforced Properties
	//-------------------------------------------------------------------------------------

	// return order property enforcing type for this operator
	virtual CEnfdProp::EPropEnforcingType EpetOrder(
		CExpressionHandle &exprhdl, const CEnfdOrder *peo) const;

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// print
	virtual IOstream &OsPrint(IOstream &) const;

	// conversion function
	static CPhysicalMotionHashDistribute *PopConvert(COperator *pop);

	virtual CDistributionSpec *PdsRequired(CMemoryPool *mp,
										   CExpressionHandle &exprhdl,
										   CDistributionSpec *pdsRequired,
										   ULONG child_index,
										   CDrvdPropArray *pdrgpdpCtxt,
										   ULONG ulOptReq) const;

};	// class CPhysicalMotionHashDistribute

}  // namespace gpopt

#endif