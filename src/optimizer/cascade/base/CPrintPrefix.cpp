//---------------------------------------------------------------------------
//	@filename:
//		CPrintPrefix.cpp
//
//	@doc:
//		Implementation of print prefix class
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CPrintPrefix.h"
#include "duckdb/optimizer/cascade/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPrintPrefix::CPrintPrefix
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CPrintPrefix::CPrintPrefix(const CPrintPrefix *ppfx, const CHAR *sz)
	: m_ppfx(ppfx), m_sz(sz)
{
	GPOS_ASSERT(NULL != sz);
}

//---------------------------------------------------------------------------
//	@function:
//		CPrintPrefix::OsPrint
//
//	@doc:
//		print function;
//		recursively traverse the linked list of prefixes and print them
//		in reverse order
//
//---------------------------------------------------------------------------
IOstream& CPrintPrefix::OsPrint(IOstream &os) const
{
	GPOS_CHECK_STACK_SIZE;

	if (NULL != m_ppfx)
	{
		(void) m_ppfx->OsPrint(os);
	}

	os << m_sz;
	return os;
}