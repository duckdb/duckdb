//---------------------------------------------------------------------------
//	@filename:
//		CMiniDumper.h
//
//	@doc:
//		Interface for minidump handler;
//---------------------------------------------------------------------------
#ifndef GPOS_CMiniDumper_H
#define GPOS_CMiniDumper_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/io/COstream.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CMiniDumper
//
//	@doc:
//		Interface for minidump handler;
//
//---------------------------------------------------------------------------
class CMiniDumper : CStackObject
{
private:
	// memory pool
	CMemoryPool *m_mp;

	// flag indicating if handler is initialized
	BOOL m_initialized;

	// flag indicating if handler is finalized
	BOOL m_finalized;

	// private copy ctor
	CMiniDumper(const CMiniDumper &);

protected:
	// stream to serialize objects to
	COstream *m_oos;

public:
	// ctor
	CMiniDumper(CMemoryPool *mp);

	// dtor
	virtual ~CMiniDumper();

	// initialize
	void Init(COstream *oos);

	// finalize
	void Finalize();

	// get stream to serialize to
	COstream &GetOStream();

	// serialize minidump header
	virtual void SerializeHeader() = 0;

	// serialize minidump footer
	virtual void SerializeFooter() = 0;

	// serialize entry header
	virtual void SerializeEntryHeader() = 0;

	// serialize entry footer
	virtual void SerializeEntryFooter() = 0;

};	// class CMiniDumper
}  // namespace gpos

#endif
