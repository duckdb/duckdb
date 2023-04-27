//---------------------------------------------------------------------------
//	@filename:
//		CLogicalCTEProducer.h
//
//	@doc:
//		Logical CTE producer operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalCTEProducer_H
#define GPOPT_CLogicalCTEProducer_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/operators/CLogical.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CLogicalCTEProducer
//
//	@doc:
//		CTE producer operator
//
//---------------------------------------------------------------------------
class CLogicalCTEProducer : public CLogical
{
private:
	// cte identifier
	ULONG m_id;

	// cte columns
	CColRefArray *m_pdrgpcr;

	// output columns, same as cte columns but in CColRefSet
	CColRefSet *m_pcrsOutput;

	// private copy ctor
	CLogicalCTEProducer(const CLogicalCTEProducer &);

public:
	// ctor
	explicit CLogicalCTEProducer(CMemoryPool *mp);

	// ctor
	CLogicalCTEProducer(CMemoryPool *mp, ULONG id, CColRefArray *colref_array);

	// dtor
	virtual ~CLogicalCTEProducer();

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopLogicalCTEProducer;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CLogicalCTEProducer";
	}

	// cte identifier
	ULONG
	UlCTEId() const
	{
		return m_id;
	}

	// cte columns
	CColRefArray *
	Pdrgpcr() const
	{
		return m_pdrgpcr;
	}

	// cte columns in CColRefSet
	CColRefSet *
	DeriveOutputColumns() const
	{
		return m_pcrsOutput;
	}

	// operator specific hash function
	virtual ULONG HashValue() const;

	// match function
	virtual BOOL Matches(COperator *pop) const;

	// sensitivity to order of inputs
	virtual BOOL
	FInputOrderSensitive() const
	{
		return false;
	}

	// return a copy of the operator with remapped columns
	virtual COperator *PopCopyWithRemappedColumns(
		CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist);

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	// derive output columns
	virtual CColRefSet *DeriveOutputColumns(CMemoryPool *mp,
											CExpressionHandle &exprhdl);

	// dervive keys
	virtual CKeyCollection *DeriveKeyCollection(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const;

	// derive max card
	virtual CMaxCard DeriveMaxCard(CMemoryPool *mp,
								   CExpressionHandle &exprhdl) const;

	// derive not nullable output columns
	virtual CColRefSet *DeriveNotNullColumns(CMemoryPool *mp,
											 CExpressionHandle &exprhdl) const;

	// derive constraint property
	virtual CPropConstraint *
	DerivePropertyConstraint(CMemoryPool *mp, CExpressionHandle &exprhdl) const
	{
		return PpcDeriveConstraintRestrict(mp, exprhdl, m_pcrsOutput);
	}

	// derive partition consumer info
	virtual CPartInfo *
	DerivePartitionInfo(CMemoryPool *,	// mp,
						CExpressionHandle &exprhdl) const
	{
		return PpartinfoPassThruOuter(exprhdl);
	}

	virtual CTableDescriptor *DeriveTableDescriptor(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const;
	// compute required stats columns of the n-th child
	virtual CColRefSet *
	PcrsStat(CMemoryPool *,		   // mp
			 CExpressionHandle &,  // exprhdl
			 CColRefSet *pcrsInput,
			 ULONG	// child_index
	) const
	{
		return PcrsStatsPassThru(pcrsInput);
	}

	// derive statistics
	virtual IStatistics *
	PstatsDerive(CMemoryPool *,	 //mp,
				 CExpressionHandle &exprhdl,
				 IStatisticsArray *	 //stats_ctxt
	) const
	{
		return PstatsPassThruOuter(exprhdl);
	}

	// stat promise
	virtual EStatPromise
	Esp(CExpressionHandle &) const
	{
		return CLogical::EspHigh;
	}

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	virtual CXformSet *PxfsCandidates(CMemoryPool *mp) const;

	//-------------------------------------------------------------------------------------

	// conversion function
	static CLogicalCTEProducer *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopLogicalCTEProducer == pop->Eopid());

		return dynamic_cast<CLogicalCTEProducer *>(pop);
	}

	// debug print
	virtual IOstream &OsPrint(IOstream &) const;

};	// class CLogicalCTEProducer

}  // namespace gpopt

#endif
