//---------------------------------------------------------------------------
//	@filename:
//		CMemoryVisitorPrint.h
//
//	@doc:
//		Memory object visitor that prints debug information for all allocated
//		objects inside a memory pool.
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CMemoryVisitorPrint_H
#define GPOS_CMemoryVisitorPrint_H

#include "duckdb/optimizer/cascade/assert.h"
#include "duckdb/optimizer/cascade/memory/IMemoryVisitor.h"
#include "duckdb/optimizer/cascade/types.h"
#include "duckdb/optimizer/cascade/utils.h"

namespace gpos
{
// specialization of memory object visitor that prints out
// the debugging information to a stream
class CMemoryVisitorPrint : public IMemoryVisitor
{
private:
	// call counter for the visit function
	ULLONG m_visits;

	// stream used for writing debug information
	IOstream &m_os;

	// private copy ctor
	CMemoryVisitorPrint(CMemoryVisitorPrint &);

public:
	// ctor
	CMemoryVisitorPrint(IOstream &os);

	// dtor
	virtual ~CMemoryVisitorPrint();

	// output information about a memory allocation
	virtual void Visit(void *user_addr, SIZE_T user_size, void *total_addr,
					   SIZE_T total_size, const CHAR *alloc_filename,
					   const ULONG alloc_line, ULLONG alloc_seq_number,
					   CStackDescriptor *stack_desc);

	// visit counter accessor
	ULLONG
	GetNumVisits() const
	{
		return m_visits;
	}
};
}  // namespace gpos

#endif