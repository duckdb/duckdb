//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalMotionGather.h
//
//	@doc:
//		Physical Gather motion operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalMotionGather_H
#define GPOPT_CPhysicalMotionGather_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/base/CDistributionSpecSingleton.h"
#include "duckdb/optimizer/cascade/base/COrderSpec.h"
#include "duckdb/optimizer/cascade/operators/CPhysicalMotion.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalMotionGather
//
//	@doc:
//		Gather motion operator
//
//---------------------------------------------------------------------------
class CPhysicalMotionGather : public CPhysicalMotion
{
private:
	// type of segment on which this gather runs (master/segment)
	CDistributionSpecSingleton *m_pdssSingeton;

	// merge spec if the operator is order-preserving
	COrderSpec *m_pos;

	// columns used by order spec
	CColRefSet *m_pcrsSort;

	// private copy ctor
	CPhysicalMotionGather(const CPhysicalMotionGather &);

public:
	// ctor
	CPhysicalMotionGather(CMemoryPool *mp,
						  CDistributionSpecSingleton::ESegmentType est);

	CPhysicalMotionGather(CMemoryPool *mp,
						  CDistributionSpecSingleton::ESegmentType est,
						  COrderSpec *pos);

	// dtor
	virtual ~CPhysicalMotionGather();

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopPhysicalMotionGather;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CPhysicalMotionGather";
	}

	CDistributionSpecSingleton::ESegmentType
	Est() const
	{
		return m_pdssSingeton->Est();
	}

	// output distribution accessor
	virtual CDistributionSpec *
	Pds() const
	{
		return m_pdssSingeton;
	}

	BOOL
	FOrderPreserving() const
	{
		return !m_pos->IsEmpty();
	}

	BOOL
	FOnMaster() const
	{
		return CDistributionSpecSingleton::EstMaster == Est();
	}

	// order spec
	COrderSpec *
	Pos() const
	{
		return m_pos;
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
	static CPhysicalMotionGather *PopConvert(COperator *pop);

};	// class CPhysicalMotionGather

}  // namespace gpopt

#endif