//---------------------------------------------------------------------------
//	@filename:
//		COrderSpec.cpp
//
//	@doc:
//		Specification of order property
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/COrderSpec.h"
#include "duckdb/optimizer/cascade/base/CColRefSet.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/operators/CPhysicalSort.h"
#include "duckdb/optimizer/cascade/error/CAutoTrace.h"
#include "duckdb/planner/operator/logical_order.hpp"

using namespace gpopt;
using namespace gpmd;

// string encoding of null treatment
const CHAR rgszNullCode[][16] = {"Auto", "NULLsFirst", "NULLsLast"};
GPOS_CPL_ASSERT(COrderSpec::EntSentinel == GPOS_ARRAY_SIZE(rgszNullCode));

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::COrderExpression::COrderExpression
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
COrderSpec::COrderExpression::COrderExpression(gpmd::IMDId *mdid, const CColRef *colref, ENullTreatment ent)
	: m_mdid(mdid), m_pcr(colref), m_ent(ent)
{
	GPOS_ASSERT(NULL != colref);
	GPOS_ASSERT(mdid->IsValid());
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::COrderExpression::~COrderExpression
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
COrderSpec::COrderExpression::~COrderExpression()
{
	m_mdid->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::COrderExpression::Matches
//
//	@doc:
//		Check if order expression equal to given one;
//
//---------------------------------------------------------------------------
BOOL COrderSpec::COrderExpression::Matches(const COrderExpression *poe) const
{
	GPOS_ASSERT(NULL != poe);

	return poe->m_mdid->Equals(m_mdid) && poe->m_pcr == m_pcr && poe->m_ent == m_ent;
}


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::COrderExpression::OsPrint
//
//	@doc:
//		Print order expression
//
//---------------------------------------------------------------------------
IOstream& COrderSpec::COrderExpression::OsPrint(IOstream &os) const
{
	os << "( ";
	m_mdid->OsPrint(os);
	os << ", ";
	m_pcr->OsPrint(os);
	os << ", " << rgszNullCode[m_ent] << " )";

	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::COrderSpec
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
COrderSpec::COrderSpec(CMemoryPool *mp) : m_mp(mp), m_pdrgpoe(NULL)
{
	m_pdrgpoe = GPOS_NEW(mp) COrderExpressionArray(mp);
}


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::~COrderSpec
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
COrderSpec::~COrderSpec()
{
	m_pdrgpoe->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::Append
//
//	@doc:
//		Append order expression;
//
//---------------------------------------------------------------------------
void
COrderSpec::Append(gpmd::IMDId *mdid, const CColRef *colref, ENullTreatment ent)
{
	COrderExpression *poe = GPOS_NEW(m_mp) COrderExpression(mdid, colref, ent);
	m_pdrgpoe->Append(poe);
}


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::Matches
//
//	@doc:
//		Check for equality between order specs
//
//---------------------------------------------------------------------------
BOOL
COrderSpec::Matches(const COrderSpec *pos) const
{
	BOOL fMatch =
		m_pdrgpoe->Size() == pos->m_pdrgpoe->Size() && FSatisfies(pos);

	GPOS_ASSERT_IMP(fMatch, pos->FSatisfies(this));

	return fMatch;
}


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::FSatisfies
//
//	@doc:
//		Check if this order spec satisfies the given one
//
//---------------------------------------------------------------------------
BOOL
COrderSpec::FSatisfies(const COrderSpec *pos) const
{
	const ULONG arity = pos->m_pdrgpoe->Size();
	BOOL fSatisfies = (m_pdrgpoe->Size() >= arity);

	for (ULONG ul = 0; fSatisfies && ul < arity; ul++)
	{
		fSatisfies = (*m_pdrgpoe)[ul]->Matches((*(pos->m_pdrgpoe))[ul]);
	}

	return fSatisfies;
}


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::AppendEnforcers
//
//	@doc:
//		Add required enforcers enforcers to dynamic array
//
//---------------------------------------------------------------------------
void COrderSpec::AppendEnforcers(CMemoryPool* mp, CExpressionHandle &exprhdl, CReqdPropPlan* prpp, CExpressionArray* pdrgpexpr, unique_ptr<LogicalOperator> pexpr)
{
	GPOS_ASSERT(NULL != prpp);
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(this == prpp->Peo()->PosRequired() && "required plan properties don't match enforced order spec");
	AddRef();
	vector<BoundOrderByNode> order;
	auto pexprSort = GPOS_NEW(mp) LogicalOrder(order);
	pexprSort->AddChild(std::move(pexpr));
	pdrgpexpr->Append(pexprSort);
}


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::HashValue
//
//	@doc:
//		Hash of components
//
//---------------------------------------------------------------------------
ULONG COrderSpec::HashValue() const
{
	ULONG ulHash = 0;
	ULONG arity = m_pdrgpoe->Size();

	for (ULONG ul = 0; ul < arity; ul++)
	{
		COrderExpression *poe = (*m_pdrgpoe)[ul];
		ulHash =
			gpos::CombineHashes(ulHash, gpos::HashPtr<CColRef>(poe->Pcr()));
	}

	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::PosCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the order spec with remapped columns
//
//---------------------------------------------------------------------------
COrderSpec* COrderSpec::PosCopyWithRemappedColumns(CMemoryPool* mp, UlongToColRefMap* colref_mapping, BOOL must_exist)
{
	COrderSpec* pos = GPOS_NEW(mp) COrderSpec(mp);
	const ULONG num_cols = m_pdrgpoe->Size();
	for (ULONG ul = 0; ul < num_cols; ul++)
	{
		COrderExpression *poe = (*m_pdrgpoe)[ul];
		IMDId *mdid = poe->GetMdIdSortOp();
		mdid->AddRef();
		const CColRef* colref = poe->Pcr();
		ULONG id = colref->Id();
		CColRef *pcrMapped = colref_mapping->Find(&id);
		if (NULL == pcrMapped)
		{
			if (must_exist)
			{
				CColumnFactory* col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
				// not found in hashmap, so create a new colref and add to hashmap
				pcrMapped = col_factory->PcrCopy(colref);
				BOOL result = colref_mapping->Insert(GPOS_NEW(mp) ULONG(id), pcrMapped);
				GPOS_ASSERT(result);
			}
			else
			{
				pcrMapped = const_cast<CColRef *>(colref);
			}
		}
		COrderSpec::ENullTreatment ent = poe->Ent();
		pos->Append(mdid, pcrMapped, ent);
	}
	return pos;
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::PosExcludeColumns
//
//	@doc:
//		Return a copy of the order spec after excluding the given columns
//
//---------------------------------------------------------------------------
COrderSpec* COrderSpec::PosExcludeColumns(CMemoryPool *mp, CColRefSet *pcrs)
{
	GPOS_ASSERT(NULL != pcrs);
	COrderSpec *pos = GPOS_NEW(mp) COrderSpec(mp);
	const ULONG num_cols = m_pdrgpoe->Size();
	for (ULONG ul = 0; ul < num_cols; ul++)
	{
		COrderExpression* poe = (*m_pdrgpoe)[ul];
		const CColRef* colref = poe->Pcr();
		if (pcrs->FMember(colref))
		{
			continue;
		}
		IMDId* mdid = poe->GetMdIdSortOp();
		mdid->AddRef();
		pos->Append(mdid, colref, poe->Ent());
	}
	return pos;
}


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::ExtractCols
//
//	@doc:
//		Extract columns from order spec into the given column set
//
//---------------------------------------------------------------------------
void
COrderSpec::ExtractCols(CColRefSet *pcrs) const
{
	GPOS_ASSERT(NULL != pcrs);

	const ULONG ulOrderExprs = m_pdrgpoe->Size();
	for (ULONG ul = 0; ul < ulOrderExprs; ul++)
	{
		pcrs->Include((*m_pdrgpoe)[ul]->Pcr());
	}
}


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::PcrsUsed
//
//	@doc:
//		Extract colref set from order components
//
//---------------------------------------------------------------------------
CColRefSet *
COrderSpec::PcrsUsed(CMemoryPool *mp) const
{
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	ExtractCols(pcrs);

	return pcrs;
}


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::GetColRefSet
//
//	@doc:
//		Extract colref set from order specs in the given array
//
//---------------------------------------------------------------------------
CColRefSet *
COrderSpec::GetColRefSet(CMemoryPool *mp, COrderSpecArray *pdrgpos)
{
	GPOS_ASSERT(NULL != pdrgpos);

	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	const ULONG ulOrderSpecs = pdrgpos->Size();
	for (ULONG ulSpec = 0; ulSpec < ulOrderSpecs; ulSpec++)
	{
		COrderSpec *pos = (*pdrgpos)[ulSpec];
		pos->ExtractCols(pcrs);
	}

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::PdrgposExclude
//
//	@doc:
//		Filter out array of order specs from order expressions using the
//		passed columns
//
//---------------------------------------------------------------------------
COrderSpecArray *
COrderSpec::PdrgposExclude(CMemoryPool *mp, COrderSpecArray *pdrgpos,
						   CColRefSet *pcrsToExclude)
{
	GPOS_ASSERT(NULL != pdrgpos);
	GPOS_ASSERT(NULL != pcrsToExclude);

	if (0 == pcrsToExclude->Size())
	{
		// no columns to exclude
		pdrgpos->AddRef();
		return pdrgpos;
	}

	COrderSpecArray *pdrgposNew = GPOS_NEW(mp) COrderSpecArray(mp);
	const ULONG ulOrderSpecs = pdrgpos->Size();
	for (ULONG ulSpec = 0; ulSpec < ulOrderSpecs; ulSpec++)
	{
		COrderSpec *pos = (*pdrgpos)[ulSpec];
		COrderSpec *posNew = pos->PosExcludeColumns(mp, pcrsToExclude);
		pdrgposNew->Append(posNew);
	}

	return pdrgposNew;
}

//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::OsPrint
//
//	@doc:
//		Print order spec
//
//---------------------------------------------------------------------------
IOstream &
COrderSpec::OsPrint(IOstream &os) const
{
	const ULONG arity = m_pdrgpoe->Size();
	if (0 == arity)
	{
		os << "<empty>";
	}
	else
	{
		for (ULONG ul = 0; ul < arity; ul++)
		{
			(*m_pdrgpoe)[ul]->OsPrint(os) << " ";
		}
	}

	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::Equals
//
//	@doc:
//		 Matching function over order spec arrays
//
//---------------------------------------------------------------------------
BOOL
COrderSpec::Equals(const COrderSpecArray *pdrgposFirst,
				   const COrderSpecArray *pdrgposSecond)
{
	if (NULL == pdrgposFirst || NULL == pdrgposSecond)
	{
		return (NULL == pdrgposFirst && NULL == pdrgposSecond);
	}

	if (pdrgposFirst->Size() != pdrgposSecond->Size())
	{
		return false;
	}

	const ULONG size = pdrgposFirst->Size();
	BOOL fMatch = true;
	for (ULONG ul = 0; fMatch && ul < size; ul++)
	{
		fMatch = (*pdrgposFirst)[ul]->Matches((*pdrgposSecond)[ul]);
	}

	return fMatch;
}


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::HashValue
//
//	@doc:
//		 Combine hash values of a maximum number of entries
//
//---------------------------------------------------------------------------
ULONG
COrderSpec::HashValue(const COrderSpecArray *pdrgpos, ULONG ulMaxSize)
{
	GPOS_ASSERT(NULL != pdrgpos);
	ULONG size = std::min(ulMaxSize, pdrgpos->Size());

	ULONG ulHash = 0;
	for (ULONG ul = 0; ul < size; ul++)
	{
		ulHash = gpos::CombineHashes(ulHash, (*pdrgpos)[ul]->HashValue());
	}

	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		COrderSpec::OsPrint
//
//	@doc:
//		 Print array of order spec objects
//
//---------------------------------------------------------------------------
IOstream &
COrderSpec::OsPrint(IOstream &os, const COrderSpecArray *pdrgpos)
{
	const ULONG size = pdrgpos->Size();
	os << "[";
	if (0 < size)
	{
		for (ULONG ul = 0; ul < size - 1; ul++)
		{
			(void) (*pdrgpos)[ul]->OsPrint(os);
			os << ", ";
		}

		(void) (*pdrgpos)[size - 1]->OsPrint(os);
	}

	return os << "]";
}