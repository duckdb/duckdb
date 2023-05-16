//---------------------------------------------------------------------------
//	@filename:
//		CLogicalIndexGet.h
//
//	@doc:
//		Basic index accessor
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalIndexGet_H
#define GPOPT_CLogicalIndexGet_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/COrderSpec.h"
#include "duckdb/optimizer/cascade/metadata/CIndexDescriptor.h"
#include "duckdb/optimizer/cascade/operators/CLogical.h"

namespace gpopt
{
// fwd declarations
class CName;
class CColRefSet;

//---------------------------------------------------------------------------
//	@class:
//		CLogicalIndexGet
//
//	@doc:
//		Basic index accessor
//
//---------------------------------------------------------------------------
class CLogicalIndexGet : public CLogical
{
private:
	// index descriptor
	CIndexDescriptor *m_pindexdesc;

	// table descriptor
	CTableDescriptor *m_ptabdesc;

	// origin operator id -- gpos::ulong_max if operator was not generated via a transformation
	ULONG m_ulOriginOpId;

	// alias for table
	const CName *m_pnameAlias;

	// output columns
	CColRefArray *m_pdrgpcrOutput;

	// set representation of output columns
	CColRefSet *m_pcrsOutput;

	// order spec
	COrderSpec *m_pos;

	// distribution columns (empty for master only tables)
	CColRefSet *m_pcrsDist;

	// private copy ctor
	CLogicalIndexGet(const CLogicalIndexGet &);

public:
	// ctors
	explicit CLogicalIndexGet(CMemoryPool *mp);

	CLogicalIndexGet(CMemoryPool *mp, const IMDIndex *pmdindex,
					 CTableDescriptor *ptabdesc, ULONG ulOriginOpId,
					 const CName *pnameAlias, CColRefArray *pdrgpcrOutput);

	// dtor
	virtual ~CLogicalIndexGet();

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopLogicalIndexGet;
	}

	// return a string for operator name
	virtual const CHAR *
	SzId() const
	{
		return "CLogicalIndexGet";
	}

	// distribution columns
	virtual const CColRefSet *
	PcrsDist() const
	{
		return m_pcrsDist;
	}

	// array of output columns
	CColRefArray *
	PdrgpcrOutput() const
	{
		return m_pdrgpcrOutput;
	}

	// origin operator id -- gpos::ulong_max if operator was not generated via a transformation
	ULONG
	UlOriginOpId() const
	{
		return m_ulOriginOpId;
	}

	// index name
	const CName &
	Name() const
	{
		return m_pindexdesc->Name();
	}

	// table alias name
	const CName &
	NameAlias() const
	{
		return *m_pnameAlias;
	}

	// index descriptor
	CIndexDescriptor *
	Pindexdesc() const
	{
		return m_pindexdesc;
	}

	// table descriptor
	CTableDescriptor *
	Ptabdesc() const
	{
		return m_ptabdesc;
	}

	// order spec
	COrderSpec *
	Pos() const
	{
		return m_pos;
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

	// derive outer references
	virtual CColRefSet *DeriveOuterReferences(CMemoryPool *mp,
											  CExpressionHandle &exprhdl);

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

	//-------------------------------------------------------------------------------------
	// Required Relational Properties
	//-------------------------------------------------------------------------------------

	// compute required stat columns of the n-th child
	virtual CColRefSet *
	PcrsStat(CMemoryPool *mp,
			 CExpressionHandle &,  // exprhdl
			 CColRefSet *,		   //pcrsInput
			 ULONG				   // child_index
	) const
	{
		// TODO:  March 26 2012; statistics derivation for indexes
		return GPOS_NEW(mp) CColRefSet(mp);
	}

	// derive statistics
	virtual IStatistics *PstatsDerive(CMemoryPool *mp,
									  CExpressionHandle &exprhdl,
									  IStatisticsArray *stats_ctxt) const;

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	CXformSet *PxfsCandidates(CMemoryPool *mp) const;

	// stat promise
	virtual EStatPromise
	Esp(CExpressionHandle &) const
	{
		return CLogical::EspLow;
	}

	//-------------------------------------------------------------------------------------
	// conversion function
	//-------------------------------------------------------------------------------------

	static CLogicalIndexGet *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopLogicalIndexGet == pop->Eopid());

		return dynamic_cast<CLogicalIndexGet *>(pop);
	}


	// debug print
	virtual IOstream &OsPrint(IOstream &) const;

};	// class CLogicalIndexGet

}  // namespace gpopt

#endif