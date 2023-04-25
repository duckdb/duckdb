//---------------------------------------------------------------------------
//	@filename:
//		CPartitionPropagationSpec.h
//
//	@doc:
//		Partition Propagation spec in required properties
//---------------------------------------------------------------------------
#ifndef GPOPT_CPartitionPropagationSpec_H
#define GPOPT_CPartitionPropagationSpec_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CHashMap.h"
#include "duckdb/optimizer/cascade/common/CRefCount.h"

#include "duckdb/optimizer/cascade/base/CPartFilterMap.h"
#include "duckdb/optimizer/cascade/base/CPartIndexMap.h"
#include "duckdb/optimizer/cascade/base/CPropSpec.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CPartitionPropagationSpec
//
//	@doc:
//		Partition Propagation specification
//
//---------------------------------------------------------------------------
class CPartitionPropagationSpec : public CPropSpec
{
private:
	// unresolved partitions map
	CPartIndexMap *m_ppim;

	// filter expressions indexed by the part index id
	CPartFilterMap *m_ppfm;

	// check if given part index id needs to be enforced on top of the given expression
	BOOL FRequiresPartitionPropagation(CMemoryPool *mp, CExpression *pexpr,
									   CExpressionHandle &exprhdl,
									   ULONG part_idx_id) const;

	// private copy ctor
	CPartitionPropagationSpec(const CPartitionPropagationSpec &);

	// split the partition elimination predicates over the various levels
	// as well as the residual predicate
	void SplitPartPredicates(CMemoryPool *mp, CExpression *pexprScalar,
							 CColRef2dArray *pdrgpdrgpcrKeys,
							 UlongToExprMap *phmulexprEqFilter,
							 UlongToExprMap *phmulexprFilter,
							 CExpression **ppexprResidual);

	// return a residual filter given an array of predicates and a bitset
	// indicating which predicates have already been used
	CExpression *PexprResidualFilter(CMemoryPool *mp,
									 CExpressionArray *pdrgpexpr,
									 CBitSet *pbsUsed);

	// return an array of predicates on the given partitioning key given
	// an array of predicates on all keys
	CExpressionArray *PdrgpexprPredicatesOnKey(CMemoryPool *mp,
											   CExpressionArray *pdrgpexpr,
											   CColRef *colref,
											   CColRefSet *pcrsKeys,
											   CBitSet **ppbs);

	// return a colrefset containing all the part keys
	CColRefSet *PcrsKeys(CMemoryPool *mp, CColRef2dArray *pdrgpdrgpcrKeys);

	// return the filter expression for the given Scan Id
	CExpression *PexprFilter(CMemoryPool *mp, ULONG scan_id);

public:
	// ctor
	CPartitionPropagationSpec(CPartIndexMap *ppim, CPartFilterMap *ppfm);

	// dtor
	virtual ~CPartitionPropagationSpec();

	// accessor of part index map
	CPartIndexMap *
	Ppim() const
	{
		return m_ppim;
	}

	// accessor of part filter map
	CPartFilterMap *
	Ppfm() const
	{
		return m_ppfm;
	}

	// append enforcers to dynamic array for the given plan properties
	virtual void AppendEnforcers(CMemoryPool *mp, CExpressionHandle &exprhdl,
								 CReqdPropPlan *prpp,
								 CExpressionArray *pdrgpexpr,
								 CExpression *pexpr);

	// hash function
	virtual ULONG HashValue() const;

	// extract columns used by the rewindability spec
	virtual CColRefSet *
	PcrsUsed(CMemoryPool *mp) const
	{
		// return an empty set
		return GPOS_NEW(mp) CColRefSet(mp);
	}

	// property type
	virtual EPropSpecType
	Epst() const
	{
		return EpstPartPropagation;
	}

	// equality function
	BOOL Matches(const CPartitionPropagationSpec *ppps) const;

	// is partition propagation required
	BOOL
	FPartPropagationReqd() const
	{
		return m_ppim->FContainsUnresolvedZeroPropagators();
	}


	// print
	IOstream &OsPrint(IOstream &os) const;

};	// class CPartitionPropagationSpec

}  // namespace gpopt

#endif
