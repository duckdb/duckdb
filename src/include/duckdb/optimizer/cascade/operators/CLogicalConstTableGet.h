//---------------------------------------------------------------------------
//	@filename:
//		CLogicalConstTableGet.h
//
//	@doc:
//		Constant table accessor
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalConstTableGet_H
#define GPOPT_CLogicalConstTableGet_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CLogical.h"

namespace gpopt
{
// dynamic array of datum arrays -- array owns elements
typedef CDynamicPtrArray<IDatumArray, CleanupRelease> IDatum2dArray;

//---------------------------------------------------------------------------
//	@class:
//		CLogicalConstTableGet
//
//	@doc:
//		Constant table accessor
//
//---------------------------------------------------------------------------
class CLogicalConstTableGet : public CLogical
{
private:
	// array of column descriptors: the schema of the const table
	CColumnDescriptorArray *m_pdrgpcoldesc;

	// array of datum arrays
	IDatum2dArray *m_pdrgpdrgpdatum;

	// output columns
	CColRefArray *m_pdrgpcrOutput;

	// private copy ctor
	CLogicalConstTableGet(const CLogicalConstTableGet &);

	// construct column descriptors from column references
	CColumnDescriptorArray *PdrgpcoldescMapping(
		CMemoryPool *mp, CColRefArray *colref_array) const;

public:
	// ctors
	explicit CLogicalConstTableGet(CMemoryPool *mp);

	CLogicalConstTableGet(CMemoryPool *mp, CColumnDescriptorArray *pdrgpcoldesc,
						  IDatum2dArray *pdrgpdrgpdatum);

	CLogicalConstTableGet(CMemoryPool *mp, CColRefArray *pdrgpcrOutput,
						  IDatum2dArray *pdrgpdrgpdatum);

	// dtor
	virtual ~CLogicalConstTableGet();

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopLogicalConstTableGet;
	}

	// return a string for operator name
	virtual const CHAR *
	SzId() const
	{
		return "CLogicalConstTableGet";
	}

	// col descr accessor
	CColumnDescriptorArray *
	Pdrgpcoldesc() const
	{
		return m_pdrgpcoldesc;
	}

	// const table values accessor
	IDatum2dArray *
	Pdrgpdrgpdatum() const
	{
		return m_pdrgpdrgpdatum;
	}

	// accessors
	CColRefArray *
	PdrgpcrOutput() const
	{
		return m_pdrgpcrOutput;
	}

	// sensitivity to order of inputs
	BOOL FInputOrderSensitive() const;

	// operator specific hash function
	virtual ULONG HashValue() const;

	// match function
	virtual BOOL Matches(COperator *pop) const;

	// return a copy of the operator with remapped columns
	virtual COperator *PopCopyWithRemappedColumns(
		CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist);

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	// derive output columns
	virtual CColRefSet *DeriveOutputColumns(CMemoryPool *, CExpressionHandle &);

	// derive max card
	virtual CMaxCard DeriveMaxCard(CMemoryPool *mp,
								   CExpressionHandle &exprhdl) const;

	// derive partition consumer info
	virtual CPartInfo *
	DerivePartitionInfo(CMemoryPool *mp,
						CExpressionHandle &	 //exprhdl
	) const
	{
		return GPOS_NEW(mp) CPartInfo(mp);
	}

	// derive constraint property
	virtual CPropConstraint *
	DerivePropertyConstraint(CMemoryPool *mp,
							 CExpressionHandle &  // exprhdl
	) const
	{
		// TODO:  - Jan 11, 2013; compute constraints based on the
		// datum values in this CTG
		return GPOS_NEW(mp) CPropConstraint(
			mp, GPOS_NEW(mp) CColRefSetArray(mp), NULL /*pcnstr*/);
	}

	//-------------------------------------------------------------------------------------
	// Required Relational Properties
	//-------------------------------------------------------------------------------------

	// compute required stat columns of the n-th child
	virtual CColRefSet *
	PcrsStat(CMemoryPool *,		   // mp
			 CExpressionHandle &,  // exprhdl
			 CColRefSet *,		   // pcrsInput
			 ULONG				   // child_index
	) const
	{
		GPOS_ASSERT(!"CLogicalConstTableGet has no children");
		return NULL;
	}

	// derive statistics
	virtual IStatistics *PstatsDerive(CMemoryPool *mp,
									  CExpressionHandle &exprhdl,
									  IStatisticsArray *stats_ctxt) const;

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	virtual CXformSet *PxfsCandidates(CMemoryPool *mp) const;

	// stat promise
	virtual EStatPromise
	Esp(CExpressionHandle &) const
	{
		return CLogical::EspLow;
	}

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// conversion function
	static CLogicalConstTableGet *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopLogicalConstTableGet == pop->Eopid());

		return dynamic_cast<CLogicalConstTableGet *>(pop);
	}


	// debug print
	virtual IOstream &OsPrint(IOstream &) const;

};	// class CLogicalConstTableGet

}  // namespace gpopt

#endif