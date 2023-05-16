//---------------------------------------------------------------------------
//	@filename:
//		CMessageTable.h
//
//	@doc:
//		Error message table;
//---------------------------------------------------------------------------
#ifndef GPOS_CMessageTable_H
#define GPOS_CMessageTable_H

#include "duckdb/optimizer/cascade/error/CMessage.h"

#define GPOS_MSGTAB_SIZE 4096

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CMessageTable
//
//	@doc:
//		Maintains error messages for a given locale
//
//---------------------------------------------------------------------------
class CMessageTable
{
	// short hand for message tables
	typedef CSyncHashtable<CMessage, CException> MessageTable;

	// short hand for message table accessor
	typedef CSyncHashtableAccessByKey<CMessage, CException> MTAccessor;

	// message hashtable
	MessageTable m_hash_table;

	// private copy ctor
	CMessageTable(const CMessageTable &);

public:
	// ctor
	CMessageTable(CMemoryPool *mp, ULONG size, ELocale locale);

	// lookup message by error/local
	CMessage *LookupMessage(CException exc);

	// insert message
	void AddMessage(CMessage *msg);

	// simple comparison
	BOOL
	operator==(const CMessageTable &mt) const
	{
		return m_locale == mt.m_locale;
	}

	// equality function -- needed for hashtable
	static BOOL
	Equals(const ELocale &locale, const ELocale &other_locale)
	{
		return locale == other_locale;
	}

	// basic hash function
	static ULONG
	HashValue(const ELocale &locale)
	{
		return (ULONG) locale;
	}

	// link object
	SLink m_link;

	// locale
	ELocale m_locale;

	// invalid locale
	static const ELocale m_invalid_locale;

};	// class CMessageTable
}  // namespace gpos

#endif