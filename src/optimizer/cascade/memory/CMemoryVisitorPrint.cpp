//---------------------------------------------------------------------------
//	@filename:
//		CMemoryVisitorPrint.cpp
//
//	@doc:
//		Implementation of memory object visitor for printing debug information
//		for all allocated objects inside a memory pool.
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/memory/CMemoryVisitorPrint.h"
#include "duckdb/optimizer/cascade/assert.h"
#include "duckdb/optimizer/cascade/common/CStackDescriptor.h"
#include "duckdb/optimizer/cascade/string/CWStringStatic.h"
#include "duckdb/optimizer/cascade/types.h"
#include "duckdb/optimizer/cascade/utils.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CMemoryVisitorPrint::CMemoryVisitorPrint
//
//	@doc:
//	  Ctor.
//
//---------------------------------------------------------------------------
CMemoryVisitorPrint::CMemoryVisitorPrint(IOstream &os) : m_visits(0), m_os(os)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryVisitorPrint::~CMemoryVisitorPrint
//
//	@doc:
//	  Dtor.
//
//---------------------------------------------------------------------------
CMemoryVisitorPrint::~CMemoryVisitorPrint()
{
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryVisitorPrint::FVisit
//
//	@doc:
//		Prints the live object information to the output stream.
//
//---------------------------------------------------------------------------
void
CMemoryVisitorPrint::Visit(void *user_addr, SIZE_T user_size, void *total_addr,
						   SIZE_T total_size, const CHAR *alloc_filename,
						   const ULONG alloc_line, ULLONG alloc_seq_number,
						   CStackDescriptor *stack_desc)
{
	m_os << COstream::EsmDec << "allocation sequence number "
		 << alloc_seq_number << ","
		 << " total size " << (ULONG) total_size << " bytes,"
		 << " base address " << total_addr << ","
		 << " user size " << (ULONG) user_size << " bytes,"
		 << " user address " << user_addr << ","
		 << " allocated by " << alloc_filename << ":" << alloc_line
		 << std::endl;

	ITask *task = ITask::Self();
	if (NULL != task)
	{
		if (NULL != stack_desc &&
			task->IsTraceSet(EtracePrintMemoryLeakStackTrace))
		{
			m_os << "Stack trace: " << std::endl;
			stack_desc->AppendTrace(m_os, 8 /*ulDepth*/);
		}

		if (task->IsTraceSet(EtracePrintMemoryLeakDump))
		{
			m_os << "Memory dump: " << std::endl;
			gpos::HexDump(m_os, total_addr, total_size);
		}
	}

	++m_visits;
}