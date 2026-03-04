//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/thread_annotation/thread_annotation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

// Enable thread safety attributes only with clang.
// The attributes can be safely erased when compiling with other compilers.
#if defined(__clang__) && (!defined(SWIG))
#define DUCKDB_THREAD_ANNOTATION_ATTRIBUTE(x) __attribute__((x))
#else
#define DUCKDB_THREAD_ANNOTATION_ATTRIBUTE(x) // no-op
#endif

// CAPABILITY is an attribute on classes, which specifies that objects of the class can be used as a capability. The
// string argument specifies the kind of capability in error messages, e.g. "mutex".
#define DUCKDB_CAPABILITY(x) DUCKDB_THREAD_ANNOTATION_ATTRIBUTE(capability(x))

// SCOPED_CAPABILITY is an attribute on classes that implement RAII-style locking, in which a capability is acquired
// in the constructor, and released in the destructor. Such classes require special handling because the
// constructor and destructor refer to the capability via different names.
#define DUCKDB_SCOPED_CAPABILITY DUCKDB_THREAD_ANNOTATION_ATTRIBUTE(scoped_lockable)
