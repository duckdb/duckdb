//---------------------------------------------------------------------------
//	@filename:
//		CLogicalSequenceProject.h
//
//	@doc:
//		Logical Sequence Project operator
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalSequenceProject_H
#define GPOS_CLogicalSequenceProject_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/COrderSpec.h"
#include "duckdb/optimizer/cascade/base/CWindowFrame.h"
#include "duckdb/optimizer/cascade/operators/CLogicalUnary.h"

namespace gpopt
{
// fwd declarations
class CDistributionSpec;

//---------------------------------------------------------------------------
//	@class:
//		CLogicalSequenceProject
//
//	@doc:
//		Logical Sequence Project operator
//
//---------------------------------------------------------------------------
class CLogicalSequenceProject : public CLogicalUnary
{
private:
	// partition by keys
	CDistributionSpec *m_pds;

	// order specs of child window functions
	COrderSpecArray *m_pdrgpos;

	// frames of child window functions
	CWindowFrameArray *m_pdrgpwf;

	// flag indicating if current operator has any non-empty order specs
	BOOL m_fHasOrderSpecs;

	// flag indicating if current operator has any non-empty frame specs
	BOOL m_fHasFrameSpecs;

	// set the flag indicating that SeqPrj has specified order specs
	void SetHasOrderSpecs(CMemoryPool *mp);

	// set the flag indicating that SeqPrj has specified frame specs
	void SetHasFrameSpecs(CMemoryPool *mp);

	// private copy ctor
	CLogicalSequenceProject(const CLogicalSequenceProject &);

public:
	// ctor
	CLogicalSequenceProject(CMemoryPool *mp, CDistributionSpec *pds,
							COrderSpecArray *pdrgpos,
							CWindowFrameArray *pdrgpwf);

	// ctor for pattern
	explicit CLogicalSequenceProject(CMemoryPool *mp);

	// dtor
	virtual ~CLogicalSequenceProject();

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopLogicalSequenceProject;
	}

	// operator name
	virtual const CHAR *
	SzId() const
	{
		return "CLogicalSequenceProject";
	}

	// distribution spec
	CDistributionSpec *
	Pds() const
	{
		return m_pds;
	}

	// order by keys
	COrderSpecArray *
	Pdrgpos() const
	{
		return m_pdrgpos;
	}

	// frame specifications
	CWindowFrameArray *
	Pdrgpwf() const
	{
		return m_pdrgpwf;
	}

	// return true if non-empty order specs are used by current operator
	BOOL
	FHasOrderSpecs() const
	{
		return m_fHasOrderSpecs;
	}

	// return true if non-empty frame specs are used by current operator
	BOOL
	FHasFrameSpecs() const
	{
		return m_fHasFrameSpecs;
	}

	// return a copy of the operator with remapped columns
	virtual COperator *PopCopyWithRemappedColumns(
		CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist);

	// return true if we can pull projections up past this operator from its given child
	virtual BOOL FCanPullProjectionsUp(ULONG  //child_index
	) const
	{
		return false;
	}

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	// derive output columns
	virtual CColRefSet *DeriveOutputColumns(CMemoryPool *mp,
											CExpressionHandle &exprhdl);

	// derive outer references
	virtual CColRefSet *DeriveOuterReferences(CMemoryPool *mp,
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

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	virtual CXformSet *PxfsCandidates(CMemoryPool *mp) const;

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// derive statistics
	virtual IStatistics *PstatsDerive(CMemoryPool *mp,
									  CExpressionHandle &exprhdl,
									  IStatisticsArray *stats_ctxt) const;

	// match function
	virtual BOOL Matches(COperator *pop) const;

	virtual ULONG HashValue() const;

	// print
	virtual IOstream &OsPrint(IOstream &os) const;

	// remove outer references from Order By/ Partition By clauses, and return a new operator
	CLogicalSequenceProject *PopRemoveLocalOuterRefs(
		CMemoryPool *mp, CExpressionHandle &exprhdl);

	// check for outer references in Partition/Order, or window frame edges
	BOOL FHasLocalReferencesTo(const CColRefSet *outerRefsToCheck) const;

	// conversion function
	static CLogicalSequenceProject *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopLogicalSequenceProject == pop->Eopid());

		return dynamic_cast<CLogicalSequenceProject *>(pop);
	}

};	// class CLogicalSequenceProject

}  // namespace gpopt

#endif