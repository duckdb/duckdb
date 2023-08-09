//---------------------------------------------------------------------------
//	@filename:
//		CStackDescriptor.h
//
//	@doc:
//		Class for stack descriptor.
//---------------------------------------------------------------------------
#ifndef CStackDescriptor_H
#define CStackDescriptor_H

#include "duckdb/optimizer/cascade/common/clibtypes.h"
#include "duckdb/optimizer/cascade/io/IOstream.h"
#include "duckdb/optimizer/cascade/types.h"

#define GPOS_STACK_TRACE_BUFFER_SIZE 4096
#define GPOS_STACK_TRACE_DEPTH 32
#define GPOS_STACK_SYMBOL_SIZE 16384
#define GPOS_x86_64 1
#define GPOS_STACK_TRACE_FORMAT_SIZE 192

namespace gpos
{
// prototype
class CWString;

class CStackDescriptor
{
private:
	// stack depth
	ULONG m_depth;

	// array with frame return addresses
	void* m_array_of_addresses[GPOS_STACK_TRACE_DEPTH];

	// append formatted symbol description
	void AppendSymbolInfo(CWString *ws, CHAR *demangling_symbol_buffer, SIZE_T size, const Dl_info &symbol_info_array, ULONG index) const;

#if (GPOS_sparc)
	//  method called by walkcontext function to store return addresses
	static INT GetStackFrames(ULONG_PTR func_ptr, INT sig __attribute__((unused)), void *context);
#endif	// GPOS_sparc

	// reset descriptor
	void Reset()
	{
		// reset stack depth
		m_depth = 0;
	}

public:
	// ctor
	CStackDescriptor() : m_depth(0)
	{
		Reset();
	}

	// store current stack skipping (top_frames_to_skip) top frames
	void BackTrace(ULONG top_frames_to_skip = 0);

	// append trace of stored stack to string
	void AppendTrace(CWString *ws, ULONG depth = GPOS_STACK_TRACE_DEPTH) const;

	// append trace of stored stack to stream
	void AppendTrace(IOstream &os, ULONG depth = GPOS_STACK_TRACE_DEPTH) const;

	// get hash value for stored stack
	ULONG HashValue() const;
};	// class CStackTrace
}  // namespace gpos
#endif