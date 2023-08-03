//---------------------------------------------------------------------------
//	@filename:
//		CStackDescriptor.cpp
//
//	@doc:
//		Implementation of interface class for execution stack tracing.
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/common/CStackDescriptor.h"
#include "duckdb/optimizer/cascade/string/CWString.h"
#include "duckdb/optimizer/cascade/task/CWorker.h"
#include "duckdb/optimizer/cascade/utils.h"

#define GPOS_STACK_DESCR_TRACE_BUF (4096)

using namespace gpos;

#if (GPOS_sparc)
//---------------------------------------------------------------------------
//	@function:
//		CStackDescriptor::GetStackFrames
//
//	@doc:
//		Method called by walkcontext function to store return addresses
//
//---------------------------------------------------------------------------
INT CStackDescriptor::GetStackFrames(ULONG_PTR func_ptr, INT sig __attribute__((unused)), void *context)
{
	CStackDescriptor* stack_descriptor = (CStackDescriptor*) context;
	// check if max number of frames has been reached
	if (stack_descriptor->m_depth < GPOS_STACK_TRACE_DEPTH)
	{
		// set frame address
		stack_descriptor->m_array_of_addresses[stack_descriptor->m_depth++] = (void *) func_ptr;
	}
	return 0;
}

//---------------------------------------------------------------------------
//	@function:
//		CStackDescriptor::BackTrace
//
//	@doc:
//		Store current stack
//
//---------------------------------------------------------------------------
void CStackDescriptor::BackTrace(ULONG top_frames_to_skip)
{
	// reset stack depth
	Reset();
	// retrieve stack context
	ucontext_t context;
	if (0 != clib::GetContext(&context))
	{
		return;
	}
	// walk stack context to get stack addresses
	if (0 != clib::WalkContext(&context, GetStackFrames, this))
	{
		return;
	}
	// skip top frames
	if (top_frames_to_skip <= m_depth)
	{
		m_depth -= top_frames_to_skip;
		for (ULONG i = 0; i < m_depth; i++)
		{
			m_array_of_addresses[i] = m_array_of_addresses[i + top_frames_to_skip];
		}
	}
}
#elif (GPOS_aarch64 || GPOS_i386 || GPOS_i686 || GPOS_x86_64)
//---------------------------------------------------------------------------
//	@function:
//		CStackDescriptor::BackTrace
//
//	@doc:
//		Store current stack
//
//---------------------------------------------------------------------------
void CStackDescriptor::BackTrace(ULONG top_frames_to_skip)
{
	// get base pointer of current frame
	ULONG_PTR current_frame;
	GPOS_GET_FRAME_POINTER(current_frame);
	// reset stack depth
	Reset();
	// pointer to next frame in stack
	void **next_frame = (void **) current_frame;
	// get stack start address
	ULONG_PTR stack_start = 0;
	CWorker* worker = CWorker::Self();
	if (nullptr == worker)
	{
		// no worker in stack, return immediately
		return;
	}
	// get address from worker
	stack_start = worker->GetStackStart();
	// consider the first GPOS_STACK_TRACE_DEPTH frames below worker object
	for (ULONG frame_counter = 0; frame_counter < GPOS_STACK_TRACE_DEPTH; frame_counter++)
	{
		// check if the frame pointer is after stack start and before previous frame
		if ((ULONG_PTR) *next_frame > stack_start || (ULONG_PTR) *next_frame < (ULONG_PTR) next_frame)
		{
			break;
		}
		// skip top frames
		if (0 < top_frames_to_skip)
		{
			top_frames_to_skip--;
		}
		else
		{
			// get return address (one above the base pointer)
			ULONG_PTR *frame_address = (ULONG_PTR *) (next_frame + 1);
			m_array_of_addresses[m_depth++] = (void *) *frame_address;
		}
		// move to next frame
		next_frame = (void **) *next_frame;
	}
}
#else  // unsupported platform
void CStackDescriptor::BackTrace(ULONG)
{
	GPOS_CPL_ASSERT(!"Backtrace is not supported for this platform");
}
#endif

//---------------------------------------------------------------------------
//	@function:
//		CStackDescriptor::AppendSymbolInfo
//
//	@doc:
//		Append formatted symbol description
//
//---------------------------------------------------------------------------
void CStackDescriptor::AppendSymbolInfo(CWString *ws, CHAR *demangling_symbol_buffer, SIZE_T size, const DL_INFO &symbol_info, ULONG index) const
{
	const CHAR* symbol_name = demangling_symbol_buffer;
	// resolve symbol name
	if (symbol_info.dli_sname)
	{
		INT status = 0;
		symbol_name = symbol_info.dli_sname;
		// demangle C++ symbol
		CHAR* demangled_symbol = clib::Demangle(symbol_name, demangling_symbol_buffer, &size, &status);
		if (0 == status)
		{
			// skip args and template symbol_info
			for (ULONG ul = 0; ul < size; ul++)
			{
				if ('(' == demangling_symbol_buffer[ul] || '<' == demangling_symbol_buffer[ul])
				{
					demangling_symbol_buffer[ul] = '\0';
					break;
				}
			}
			symbol_name = demangled_symbol;
		}
	}
	else
	{
		symbol_name = "<symbol not found>";
	}
	// format symbol symbol_info
	ws->AppendFormat( GPOS_WSZ_LIT("%-4d 0x%016lx %s + %lu\n"), index + 1, (long unsigned int) m_array_of_addresses[index], symbol_name, (long unsigned int) m_array_of_addresses[index] - (ULONG_PTR) symbol_info.dli_saddr);
}

//---------------------------------------------------------------------------
//	@function:
//		CStackDescriptor::AppendTrace
//
//	@doc:
//		Append trace of stored stack to string
//
//---------------------------------------------------------------------------
void CStackDescriptor::AppendTrace(CWString *ws, ULONG depth) const
{
	// symbol symbol_info
	Dl_info symbol_info;
	// buffer for symbol demangling
	CHAR demangling_symbol_buffer[GPOS_STACK_SYMBOL_SIZE];
	// print symbol_info for frames in stack
	for (ULONG i = 0; i < m_depth && i < depth; i++)
	{
		// resolve address
		clib::Dladdr(m_array_of_addresses[i], &symbol_info);
		// get symbol description
		AppendSymbolInfo(ws, demangling_symbol_buffer, GPOS_STACK_SYMBOL_SIZE, symbol_info, i);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CStackDescriptor::AppendTrace
//
//	@doc:
//		Append trace of stored stack to stream
//
//---------------------------------------------------------------------------
void CStackDescriptor::AppendTrace(IOstream &os, ULONG depth) const
{
	WCHAR wsz[GPOS_STACK_DESCR_TRACE_BUF];
	CWStringStatic str(wsz, GPOS_ARRAY_SIZE(wsz));
	AppendTrace(&str, depth);
	os << str.GetBuffer();
}

//---------------------------------------------------------------------------
//	@function:
//		CStackDescriptor::HashValue
//
//	@doc:
//		Get hash value for stored stack
//
//---------------------------------------------------------------------------
ULONG CStackDescriptor::HashValue() const
{
	return gpos::HashByteArray((BYTE *) m_array_of_addresses, m_depth * GPOS_SIZEOF(m_array_of_addresses[0]));
}