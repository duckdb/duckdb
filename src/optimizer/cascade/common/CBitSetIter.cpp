//---------------------------------------------------------------------------
//	@filename:
//		CBitSetIter.cpp
//
//	@doc:
//		Implementation of bitset iterator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/common/CBitSetIter.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CAutoRef.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CBitSetIter::CBitSetIter
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CBitSetIter::CBitSetIter(const CBitSet &bs)
	: m_bs(bs), m_cursor((ULONG) -1), m_bsl(NULL), m_active(true)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSetIter::Advance
//
//	@doc:
//		Move to next bit
//
//---------------------------------------------------------------------------
BOOL CBitSetIter::Advance()
{
	GPOS_ASSERT(m_active && "called advance on exhausted iterator");

	if (NULL == m_bsl)
	{
		m_bsl = m_bs.m_bsllist.First();
	}

	while (NULL != m_bsl)
	{
		if (m_cursor + 1 <= m_bs.m_vector_size &&
			m_bsl->GetVec()->GetNextSetBit(m_cursor + 1, m_cursor))
		{
			break;
		}

		m_bsl = m_bs.m_bsllist.Next(m_bsl);
		m_cursor = (ULONG) -1;
	}

	m_active = (NULL != m_bsl);
	return m_active;
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSetIter::UlBit
//
//	@doc:
//		Return current position of cursor
//
//---------------------------------------------------------------------------
ULONG CBitSetIter::Bit() const
{
	GPOS_ASSERT(m_active && NULL != m_bsl && "iterator uninitialized");
	GPOS_ASSERT(m_bsl->GetVec()->Get(m_cursor));

	return m_bsl->GetOffset() + m_cursor;
}