//---------------------------------------------------------------------------
//	@filename:
//		CBitSet.cpp
//
//	@doc:
//		Implementation of bit sets
//
//		Underlying assumption: a set contains only a few links of bitvectors
//		hence, keeping them in a linked list is efficient;
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/common/CBitSet.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CAutoRef.h"
#include "duckdb/optimizer/cascade/common/CBitSetIter.h"

#ifdef GPOS_DEBUG
#include "duckdb/optimizer/cascade/error/CAutoTrace.h"
#endif	// GPOS_DEBUG

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CBitSetLink
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CBitSet::CBitSetLink::CBitSetLink(CMemoryPool *mp, ULONG offset, ULONG vector_size)
	: m_offset(offset)
{
	m_vec = GPOS_NEW(mp) CBitVector(mp, vector_size);
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSetLink
//
//	@doc:
//		copy ctor
//
//---------------------------------------------------------------------------
CBitSet::CBitSetLink::CBitSetLink(CMemoryPool *mp, const CBitSetLink &bsl)
	: m_offset(bsl.m_offset)
{
	m_vec = GPOS_NEW(mp) CBitVector(mp, *bsl.GetVec());
}


//---------------------------------------------------------------------------
//	@function:
//		~CBitSetLink
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CBitSet::CBitSetLink::~CBitSetLink()
{
	GPOS_DELETE(m_vec);
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::FindLinkByOffset
//
//	@doc:
//		Find bit set link for a given offset; if non-existent return previous
//		link (may be NULL);
//		By providing a starting link we can implement a number of operations
//		in one sweep, ie O(N)
//
//---------------------------------------------------------------------------
CBitSet::CBitSetLink* CBitSet::FindLinkByOffset(ULONG offset, CBitSetLink *bsl) const
{
	CBitSetLink *found = NULL;
	CBitSetLink *cursor = bsl;

	if (NULL == bsl)
	{
		// if no cursor provided start with first element
		cursor = m_bsllist.First();
	}
	else
	{
		GPOS_ASSERT(bsl->GetOffset() <= offset && "invalid start cursor");
		found = bsl;
	}

	GPOS_ASSERT_IMP(NULL != cursor, GPOS_OK == m_bsllist.Find(cursor) && "cursor not in list");

	while (1)
	{
		// no more links or we've overshot the target
		if (NULL == cursor || cursor->GetOffset() > offset)
		{
			break;
		}

		found = cursor;
		cursor = m_bsllist.Next(cursor);
	}

	GPOS_ASSERT_IMP(found, found->GetOffset() <= offset);
	return found;
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::RecomputeSize
//
//	@doc:
//		Compute size of set by adding up sizes of links
//
//---------------------------------------------------------------------------
void
CBitSet::RecomputeSize()
{
	m_size = 0;
	CBitSetLink *bsl = NULL;

	for (bsl = m_bsllist.First(); bsl != NULL; bsl = m_bsllist.Next(bsl))
	{
		m_size += bsl->GetVec()->CountSetBits();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::Clear
//
//	@doc:
//		release all links
//
//---------------------------------------------------------------------------
void
CBitSet::Clear()
{
	CBitSetLink *bsl = NULL;

	while (NULL != (bsl = m_bsllist.First()))
	{
		CBitSetLink *bsl_to_remove = bsl;
		bsl = m_bsllist.Next(bsl);

		m_bsllist.Remove(bsl_to_remove);
		GPOS_DELETE(bsl_to_remove);
	}

	RecomputeSize();
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::GetOffset
//
//	@doc:
//		Compute offset
//
//---------------------------------------------------------------------------
ULONG
CBitSet::ComputeOffset(ULONG ul) const
{
	return (ul / m_vector_size) * m_vector_size;
}



//---------------------------------------------------------------------------
//	@function:
//		CBitSet::CBitSet
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CBitSet::CBitSet(CMemoryPool *mp, ULONG vector_size)
	: m_mp(mp), m_vector_size(vector_size), m_size(0)
{
	m_bsllist.Init(GPOS_OFFSET(CBitSetLink, m_link));
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::CBitSet
//
//	@doc:
//		copy ctor;
//
//---------------------------------------------------------------------------
CBitSet::CBitSet(CMemoryPool *mp, const CBitSet &bs)
	: m_mp(mp), m_vector_size(bs.m_vector_size), m_size(0)
{
	m_bsllist.Init(GPOS_OFFSET(CBitSetLink, m_link));
	Union(&bs);
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::~CBitSet
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CBitSet::~CBitSet()
{
	Clear();
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::Get
//
//	@doc:
//		Check if given bit is set
//
//---------------------------------------------------------------------------
BOOL
CBitSet::Get(ULONG pos) const
{
	ULONG offset = ComputeOffset(pos);

	CBitSetLink *bsl = FindLinkByOffset(offset);
	if (NULL != bsl && bsl->GetOffset() == offset)
	{
		return bsl->GetVec()->Get(pos - offset);
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::ExchangeSet
//
//	@doc:
//		Set given bit; return previous value; allocate new link if necessary
//
//---------------------------------------------------------------------------
BOOL
CBitSet::ExchangeSet(ULONG pos)
{
	ULONG offset = ComputeOffset(pos);

	CBitSetLink *bsl = FindLinkByOffset(offset);
	if (NULL == bsl || bsl->GetOffset() != offset)
	{
		CBitSetLink *pbsl_new =
			GPOS_NEW(m_mp) CBitSetLink(m_mp, offset, m_vector_size);
		if (NULL == bsl)
		{
			m_bsllist.Prepend(pbsl_new);
		}
		else
		{
			// insert after found link
			m_bsllist.Append(pbsl_new, bsl);
		}

		bsl = pbsl_new;
	}

	GPOS_ASSERT(bsl->GetOffset() == offset);

	BOOL bit = bsl->GetVec()->ExchangeSet(pos - offset);
	if (!bit)
	{
		m_size++;
	}

	return bit;
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::ExchangeClear
//
//	@doc:
//		Clear given bit; return previous value
//
//---------------------------------------------------------------------------
BOOL
CBitSet::ExchangeClear(ULONG pos)
{
	ULONG offset = ComputeOffset(pos);

	CBitSetLink *bsl = FindLinkByOffset(offset);
	if (NULL != bsl && bsl->GetOffset() == offset)
	{
		BOOL bit = bsl->GetVec()->ExchangeClear(pos - offset);

		// remove empty link
		if (bsl->GetVec()->IsEmpty())
		{
			m_bsllist.Remove(bsl);
			GPOS_DELETE(bsl);
		}

		if (bit)
		{
			m_size--;
		}

		return bit;
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::Union
//
//	@doc:
//		Union with given other set;
//		(1) determine which links need to be allocated before(!) modifying
//			the set allocate and copy missing links aside
//		(2) insert the new links into the list
//		(3) union all links, old and new, on a per-bitvector basis
//
//		For clarity step (2) and (3) are separated;
//
//---------------------------------------------------------------------------
void
CBitSet::Union(const CBitSet *pbsOther)
{
	CBitSetLink *bsl = NULL;
	CBitSetLink *bsl_other = NULL;

	// dynamic array of CBitSetLink
	typedef CDynamicPtrArray<CBitSetLink, CleanupNULL> CBitSetLinkArray;

	CAutoRef<CBitSetLinkArray> a_drgpbsl;
	a_drgpbsl = GPOS_NEW(m_mp) CBitSetLinkArray(m_mp);

	// iterate through other's links and copy missing links to array
	for (bsl_other = pbsOther->m_bsllist.First(); bsl_other != NULL;
		 bsl_other = pbsOther->m_bsllist.Next(bsl_other))
	{
		bsl = FindLinkByOffset(bsl_other->GetOffset(), bsl);
		if (NULL == bsl || bsl->GetOffset() != bsl_other->GetOffset())
		{
			// need to copy this link
			CAutoP<CBitSetLink> a_pbsl;
			a_pbsl = GPOS_NEW(m_mp) CBitSetLink(m_mp, *bsl_other);
			a_drgpbsl->Append(a_pbsl.Value());

			a_pbsl.Reset();
		}
	}

	// insert all new links
	bsl = NULL;
	for (ULONG i = 0; i < a_drgpbsl->Size(); i++)
	{
		CBitSetLink *pbslInsert = (*a_drgpbsl)[i];
		bsl = FindLinkByOffset(pbslInsert->GetOffset(), bsl);

		GPOS_ASSERT_IMP(NULL != bsl,
						bsl->GetOffset() < pbslInsert->GetOffset());
		if (NULL == bsl)
		{
			m_bsllist.Prepend(pbslInsert);
		}
		else
		{
			m_bsllist.Append(pbslInsert, bsl);
		}
	}

	// iterate through all links and union them up
	bsl_other = NULL;
	bsl = m_bsllist.First();
	while (NULL != bsl)
	{
		bsl_other = pbsOther->FindLinkByOffset(bsl->GetOffset(), bsl_other);
		if (NULL != bsl_other && bsl_other->GetOffset() == bsl->GetOffset())
		{
			bsl->GetVec()->Or(bsl_other->GetVec());
		}

		bsl = m_bsllist.Next(bsl);
	}

	RecomputeSize();
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::Intersection
//
//	@doc:
//		Iterate through all links and intersect them; release unused links
//
//---------------------------------------------------------------------------
void
CBitSet::Intersection(const CBitSet *pbsOther)
{
	if (NULL == pbsOther)
	{
		return;
	}

	CBitSetLink *bsl_other = NULL;
	CBitSetLink *bsl = m_bsllist.First();

	while (NULL != bsl)
	{
		CBitSetLink *bsl_to_remove = NULL;

		bsl_other = pbsOther->FindLinkByOffset(bsl->GetOffset(), bsl_other);
		if (NULL != bsl_other && bsl_other->GetOffset() == bsl->GetOffset())
		{
			bsl->GetVec()->And(bsl_other->GetVec());
			bsl = m_bsllist.Next(bsl);
		}
		else
		{
			bsl_to_remove = bsl;
			bsl = m_bsllist.Next(bsl);

			m_bsllist.Remove(bsl_to_remove);
			GPOS_DELETE(bsl_to_remove);
		}
	}

	RecomputeSize();
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::Difference
//
//	@doc:
//		Substract other set from this by iterating through other set and
//		explicit removal of elements;
//
//---------------------------------------------------------------------------
void
CBitSet::Difference(const CBitSet *pbs)
{
	if (IsDisjoint(pbs))
	{
		return;
	}

	CBitSetIter bsiter(*pbs);
	while (bsiter.Advance())
	{
		(void) ExchangeClear(bsiter.Bit());
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::FSubset
//
//	@doc:
//		Determine if given vector is subset
//
//---------------------------------------------------------------------------
BOOL
CBitSet::ContainsAll(const CBitSet *bs) const
{
	// skip iterating if we can already tell by the sizes
	if (Size() < bs->Size())
	{
		return false;
	}

	CBitSetLink *bsl = NULL;
	CBitSetLink *bsl_other = NULL;

	// iterate through other's links and check for subsets
	for (bsl_other = bs->m_bsllist.First(); bsl_other != NULL;
		 bsl_other = bs->m_bsllist.Next(bsl_other))
	{
		bsl = FindLinkByOffset(bsl_other->GetOffset(), bsl);

		if (NULL == bsl || bsl->GetOffset() != bsl_other->GetOffset() ||
			!bsl->GetVec()->ContainsAll(bsl_other->GetVec()))
		{
			return false;
		}
	}

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::Equals
//
//	@doc:
//		Determine if equal
//
//---------------------------------------------------------------------------
BOOL
CBitSet::Equals(const CBitSet *bs) const
{
	// check pointer equality first
	if (this == bs)
	{
		return true;
	}

	// skip iterating if we can already tell by the sizes
	if (Size() != bs->Size())
	{
		return false;
	}

	CBitSetLink *bsl = m_bsllist.First();
	CBitSetLink *bsl_other = bs->m_bsllist.First();

	while (NULL != bsl)
	{
		if (NULL == bsl_other || bsl->GetOffset() != bsl_other->GetOffset() ||
			!bsl->GetVec()->Equals(bsl_other->GetVec()))
		{
			return false;
		}

		bsl = m_bsllist.Next(bsl);
		bsl_other = bs->m_bsllist.Next(bsl_other);
	}

	// same length implies bsl_other must have reached end as well
	return bsl_other == NULL;
}



//---------------------------------------------------------------------------
//	@function:
//		CBitSet::FDisjoint
//
//	@doc:
//		Determine if disjoint
//
//---------------------------------------------------------------------------
BOOL
CBitSet::IsDisjoint(const CBitSet *bs) const
{
	CBitSetLink *bsl = NULL;
	CBitSetLink *bsl_other = NULL;

	// iterate through other's links an check if disjoint
	for (bsl_other = bs->m_bsllist.First(); bsl_other != NULL;
		 bsl_other = bs->m_bsllist.Next(bsl_other))
	{
		bsl = FindLinkByOffset(bsl_other->GetOffset(), bsl);

		if (NULL != bsl && bsl->GetOffset() == bsl_other->GetOffset() &&
			!bsl->GetVec()->IsDisjoint(bsl_other->GetVec()))
		{
			return false;
		}
	}

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::HashValue
//
//	@doc:
//		Compute hash value for set
//
//---------------------------------------------------------------------------
ULONG
CBitSet::HashValue() const
{
	ULONG ulHash = 0;

	CBitSetLink *bsl = m_bsllist.First();
	while (NULL != bsl)
	{
		ulHash = gpos::CombineHashes(ulHash, bsl->GetVec()->HashValue());
		bsl = m_bsllist.Next(bsl);
	}

	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::OsPrint
//
//	@doc:
//		Debug print function
//
//---------------------------------------------------------------------------
IOstream &
CBitSet::OsPrint(IOstream &os) const
{
	os << "{";
	ULONG ulElems = Size();
	CBitSetIter bsiter(*this);
	for (ULONG ul = 0; ul < ulElems; ul++)
	{
		(void) bsiter.Advance();
		os << bsiter.Bit();

		if (ul < ulElems - 1)
		{
			os << ", ";
		}
	}
	os << "}";
	return os;
}