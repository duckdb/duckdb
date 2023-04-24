//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		traceflags.h
//
//	@doc:
//		enum of traceflags which can be used in a task's context
//---------------------------------------------------------------------------
#ifndef GPOS_traceflags_H
#define GPOS_traceflags_H


namespace gpos
{
enum ETraceFlag
{
	// reserve range 0-99999 for GPOS

	// test flag
	EtraceTest = 0,

	// enable OOM simulation
	EtraceSimulateOOM = 1,

	// enable Abort simulation
	EtraceSimulateAbort = 2,

	// enable I/O error simulation
	EtraceSimulateIOError = 3,

	// enable I/O error simulation
	EtraceSimulateNetError = 4,

	// disable printing memory leaks
	EtraceDisablePrintMemoryLeak = 100,

	// dump leaked memory
	EtracePrintMemoryLeakDump = 101,

	// print stack trace of leaked memory allocation
	EtracePrintMemoryLeakStackTrace = 102,

	// test memory pools for internal leaks
	EtraceTestMemoryPools = 103,

	// print exception on raise to stderr
	EtracePrintExceptionOnRaise = 104,

	EtraceSentinel
};
}

#endif
