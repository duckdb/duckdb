//---------------------------------------------------------------------------
//	@filename:
//		CMessageTable.cpp
//
//	@doc:
//		Implements message tables
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/error/CMessageTable.h"
#include "duckdb/optimizer/cascade/common/CSyncHashtableAccessByKey.h"
#include "duckdb/optimizer/cascade/common/clibwrapper.h"
#include "duckdb/optimizer/cascade/utils.h"

using namespace gpos;

// invalid locale
const ELocale CMessageTable::m_invalid_locale = ELocInvalid;

//---------------------------------------------------------------------------
//	@function:
//		CMessageTable::CMessageTable
//
//	@doc:
//
//---------------------------------------------------------------------------
CMessageTable::CMessageTable(CMemoryPool *mp, ULONG size, ELocale locale)
	: m_locale(locale)
{
	m_hash_table.Init(mp, size, GPOS_OFFSET(CMessage, m_link), GPOS_OFFSET(CMessage, m_exception), &(CException::m_invalid_exception), CException::HashValue, CException::Equals);
}

//---------------------------------------------------------------------------
//	@function:
//		CMessageTable::LookupMessage
//
//	@doc:
//		Lookup message
//
//---------------------------------------------------------------------------
CMessage* CMessageTable::LookupMessage(CException exc)
{
	MTAccessor acc(m_hash_table, exc);
	return acc.Find();
}

//---------------------------------------------------------------------------
//	@function:
//		CMessageTable::AddMessage
//
//	@doc:
//		Insert new message
//
//---------------------------------------------------------------------------
void CMessageTable::AddMessage(CMessage *msg)
{
	MTAccessor acc(m_hash_table, msg->m_exception);
	if (NULL == acc.Find())
	{
		acc.Insert(msg);
	}
	// TODO: 6/24/2010; raise approp. error for duplicate message
	// or simply ignore?
}