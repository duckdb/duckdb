//---------------------------------------------------------------------------
//	@filename:
//		CLogicalGet.h
//
//	@doc:
//		Basic table accessor
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalGet_H
#define GPOPT_CLogicalGet_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CLogical.h"

namespace gpopt
{
// fwd declarations
class CTableDescriptor;
class CName;
class CColRefSet;

//---------------------------------------------------------------------------
//	@class:
//		CLogicalGet
//
//	@doc:
//		Basic table accessor
//
//---------------------------------------------------------------------------
class CLogicalGet : public CLogical
{
private:
	// alias for table
	const CName *m_pnameAlias;

	// table descriptor
	CTableDescriptor *m_ptabdesc;

	// output columns
	CColRefArray *m_pdrgpcrOutput;

	// partition keys
	CColRef2dArray *m_pdrgpdrgpcrPart;

	// distribution columns (empty for master only tables)
	CColRefSet *m_pcrsDist;

	void CreatePartCols(CMemoryPool *mp, const ULongPtrArray *pdrgpulPart);

	// private copy ctor
	CLogicalGet(const CLogicalGet &);

public:
	// ctors
	explicit CLogicalGet(CMemoryPool *mp);

	CLogicalGet(CMemoryPool *mp, const CName *pnameAlias,
				CTableDescriptor *ptabdesc);

	CLogicalGet(CMemoryPool *mp, const CName *pnameAlias,
				CTableDescriptor *ptabdesc, CColRefArray *pdrgpcrOutput);

	// dtor
	virtual ~CLogicalGet();

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopLogicalGet;
	}

	// distribution columns
	virtual const CColRefSet *
	PcrsDist() const
	{
		return m_pcrsDist;
	}

	// return a string for operator name
	virtual const CHAR *
	SzId() const
	{
		return "CLogicalGet";
	}

	// accessors
	CColRefArray *
	PdrgpcrOutput() const
	{
		return m_pdrgpcrOutput;
	}

	// return table's name
	const CName &
	Name() const
	{
		return *m_pnameAlias;
	}

	// return table's descriptor
	CTableDescriptor *
	Ptabdesc() const
	{
		return m_ptabdesc;
	}

	// partition columns
	CColRef2dArray *
	PdrgpdrgpcrPartColumns() const
	{
		return m_pdrgpdrgpcrPart;
	}

	// operator specific hash function
	virtual ULONG HashValue() const;

	// match function
	BOOL Matches(COperator *pop) const;

	// sensitivity to order of inputs
	BOOL FInputOrderSensitive() const;

	// return a copy of the operator with remapped columns
	virtual COperator *PopCopyWithRemappedColumns(
		CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist);

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	// derive output columns
	virtual CColRefSet *DeriveOutputColumns(CMemoryPool *mp,
											CExpressionHandle &exprhdl);

	// derive not nullable output columns
	virtual CColRefSet *DeriveNotNullColumns(CMemoryPool *mp,
											 CExpressionHandle &exprhdl) const;

	// derive partition consumer info
	virtual CPartInfo *
	DerivePartitionInfo(CMemoryPool *mp,
						CExpressionHandle &	 // exprhdl
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
		return PpcDeriveConstraintFromTable(mp, m_ptabdesc, m_pdrgpcrOutput);
	}

	// derive join depth
	virtual ULONG
	DeriveJoinDepth(CMemoryPool *,		 // mp
					CExpressionHandle &	 // exprhdl
	) const
	{
		return 1;
	}

	// derive table descriptor
	virtual CTableDescriptor *
	DeriveTableDescriptor(CMemoryPool *,	   // mp
						  CExpressionHandle &  // exprhdl
	) const
	{
		return m_ptabdesc;
	}

	//-------------------------------------------------------------------------------------
	// Required Relational Properties
	//-------------------------------------------------------------------------------------

	// compute required stat columns of the n-th child
	virtual CColRefSet *
	PcrsStat(CMemoryPool *,		   // mp,
			 CExpressionHandle &,  // exprhdl
			 CColRefSet *,		   // pcrsInput
			 ULONG				   // child_index
	) const
	{
		GPOS_ASSERT(!"CLogicalGet has no children");
		return NULL;
	}

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	virtual CXformSet *PxfsCandidates(CMemoryPool *mp) const;

	// derive key collections
	virtual CKeyCollection *DeriveKeyCollection(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const;

	// derive statistics
	virtual IStatistics *PstatsDerive(CMemoryPool *mp,
									  CExpressionHandle &exprhdl,
									  IStatisticsArray *stats_ctxt) const;

	// stat promise
	virtual EStatPromise
	Esp(CExpressionHandle &) const
	{
		return CLogical::EspHigh;
	}

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// conversion function
	static CLogicalGet *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopLogicalGet == pop->Eopid() ||
					EopLogicalExternalGet == pop->Eopid());

		return dynamic_cast<CLogicalGet *>(pop);
	}

	// debug print
	virtual IOstream &OsPrint(IOstream &) const;

};	// class CLogicalGet

}  // namespace gpopt

#endif