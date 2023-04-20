//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2020 VMware, Inc.
//
//	@filename:
//		CPhysicalIndexOnlyScan.h
//
//	@doc:
//		Base class for physical index only scan operators
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalIndexOnlyScan_H
#define GPOPT_CPhysicalIndexOnlyScan_H

#include "gpos/base.h"

#include "gpopt/metadata/CIndexDescriptor.h"
#include "gpopt/operators/CPhysicalScan.h"

namespace gpopt
{
// fwd declarations
class CName;
class CDistributionSpecHashed;

//---------------------------------------------------------------------------
//	@class:
//		CPhysicalIndexOnlyScan
//
//	@doc:
//		Base class for physical index only scan operators
//
//---------------------------------------------------------------------------
class CPhysicalIndexOnlyScan : public CPhysicalScan
{
private:
	// index descriptor
	CIndexDescriptor *m_pindexdesc;

	// origin operator id -- gpos::ulong_max if operator was not generated via a transformation
	ULONG m_ulOriginOpId;

	// order
	COrderSpec *m_pos;

public:
	CPhysicalIndexOnlyScan(const CPhysicalIndexOnlyScan &) = delete;

	// ctors
	CPhysicalIndexOnlyScan(CMemoryPool *mp, CIndexDescriptor *pindexdesc,
						   CTableDescriptor *ptabdesc, ULONG ulOriginOpId,
						   const CName *pnameAlias, CColRefArray *colref_array,
						   COrderSpec *pos);

	// dtor
	~CPhysicalIndexOnlyScan() override;


	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopPhysicalIndexOnlyScan;
	}

	// operator name
	const CHAR *
	SzId() const override
	{
		return "CPhysicalIndexOnlyScan";
	}

	// table alias name
	const CName &
	NameAlias() const
	{
		return *m_pnameAlias;
	}

	// origin operator id -- gpos::ulong_max if operator was not generated via a transformation
	ULONG
	UlOriginOpId() const
	{
		return m_ulOriginOpId;
	}

	// operator specific hash function
	ULONG HashValue() const override;

	// match function
	BOOL Matches(COperator *pop) const override;

	// index descriptor
	CIndexDescriptor *
	Pindexdesc() const
	{
		return m_pindexdesc;
	}

	// sensitivity to order of inputs
	BOOL
	FInputOrderSensitive() const override
	{
		return true;
	}

	//-------------------------------------------------------------------------------------
	// Derived Plan Properties
	//-------------------------------------------------------------------------------------

	// derive sort order
	COrderSpec *
	PosDerive(CMemoryPool *,	   //mp
			  CExpressionHandle &  //exprhdl
	) const override
	{
		m_pos->AddRef();
		return m_pos;
	}

	CRewindabilitySpec *
	PrsDerive(CMemoryPool *mp,
			  CExpressionHandle &  // exprhdl
	) const override
	{
		// rewindability of output is always true
		return GPOS_NEW(mp)
			CRewindabilitySpec(CRewindabilitySpec::ErtMarkRestore,
							   CRewindabilitySpec::EmhtNoMotion);
	}

	//-------------------------------------------------------------------------------------
	// Enforced Properties
	//-------------------------------------------------------------------------------------

	// return order property enforcing type for this operator
	CEnfdProp::EPropEnforcingType EpetOrder(
		CExpressionHandle &exprhdl, const CEnfdOrder *peo) const override;

	// conversion function
	static CPhysicalIndexOnlyScan *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopPhysicalIndexOnlyScan == pop->Eopid());

		return dynamic_cast<CPhysicalIndexOnlyScan *>(pop);
	}

	// statistics derivation during costing
	IStatistics *
	PstatsDerive(CMemoryPool *,		   // mp
				 CExpressionHandle &,  // exprhdl
				 CReqdPropPlan *,	   // prpplan
				 IStatisticsArray *	   //stats_ctxt
	) const override
	{
		GPOS_ASSERT(
			!"stats derivation during costing for index only scan is invalid");

		return NULL;
	}

	// debug print
	IOstream &OsPrint(IOstream &) const override;

};	// class CPhysicalIndexOnlyScan

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalIndexOnlyScan_H

// EOF
