//---------------------------------------------------------------------------
//	@filename:
//		CLogicalCTEAnchor.h
//
//	@doc:
//		Logical CTE anchor operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalCTEAnchor_H
#define GPOPT_CLogicalCTEAnchor_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/operators/CLogical.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CLogicalCTEAnchor
//
//	@doc:
//		CTE anchor operator
//
//---------------------------------------------------------------------------
class CLogicalCTEAnchor : public CLogical
{
private:
	// cte identifier
	ULONG m_id;

	// private copy ctor
	CLogicalCTEAnchor(const CLogicalCTEAnchor &);

public:
	// ctor
	explicit CLogicalCTEAnchor(CMemoryPool *mp);

	// ctor
	CLogicalCTEAnchor(CMemoryPool *mp, ULONG id);

	// dtor
	virtual ~CLogicalCTEAnchor()
	{
	}

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopLogicalCTEAnchor;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CLogicalCTEAnchor";
	}

	// cte identifier
	ULONG
	Id() const
	{
		return m_id;
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
	virtual COperator *
	PopCopyWithRemappedColumns(CMemoryPool *,		//mp,
							   UlongToColRefMap *,	//colref_mapping,
							   BOOL					//must_exist
	)
	{
		return PopCopyDefault();
	}

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

	// derive constraint property
	virtual CPropConstraint *
	DerivePropertyConstraint(CMemoryPool *,	 //mp,
							 CExpressionHandle &exprhdl) const
	{
		return PpcDeriveConstraintPassThru(exprhdl, 0 /*ulChild*/);
	}

	// derive partition consumer info
	virtual CPartInfo *DerivePartitionInfo(CMemoryPool *mp,
										   CExpressionHandle &exprhdl) const;

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
	static CLogicalCTEAnchor *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopLogicalCTEAnchor == pop->Eopid());

		return dynamic_cast<CLogicalCTEAnchor *>(pop);
	}

	// debug print
	virtual IOstream &OsPrint(IOstream &) const;

};	// class CLogicalCTEAnchor

}  // namespace gpopt

#endif