//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/thread_annotation.hpp
//
//
//===----------------------------------------------------------------------===//

// Thread annotatio which enables clang thread safety analysis, a C++ language extension which warns about potential
// race conditions in code. The analysis is completely static (i.e. compile-time); there is no run-time overhead. For
// more information, please refer to: https://clang.llvm.org/docs/ThreadSafetyAnalysis.html#reference-guide

#pragma once

#include "duckdb/common/thread_annotation/thread_annotation.hpp"

// GUARDED_BY is an attribute on data members, which declares that the data member is protected by the given
// capability. Read operations on the data require shared access, while write operations require exclusive access.
#define DUCKDB_GUARDED_BY(x) DUCKDB_THREAD_ANNOTATION_ATTRIBUTE(guarded_by(x))

// PT_GUARDED_BY is similar, but is intended for use on pointers and smart pointers. There is no constraint on the data
// member itself, but the data that it points to is protected by the given capability.
#define DUCKDB_PT_GUARDED_BY(x) DUCKDB_THREAD_ANNOTATION_ATTRIBUTE(pt_guarded_by(x))

// REQUIRES is an attribute on functions or methods, which declares that the calling thread must have exclusive access
// to the given capabilities. More than one capability may be specified. The capabilities must be held on entry to the
// function, and must still be held on exit. REQUIRES_SHARED is similar, but requires only shared access.
#define DUCKDB_REQUIRES(...)        DUCKDB_THREAD_ANNOTATION_ATTRIBUTE(requires_capability(__VA_ARGS__))
#define DUCKDB_REQUIRES_SHARED(...) DUCKDB_THREAD_ANNOTATION_ATTRIBUTE(requires_shared_capability(__VA_ARGS__))

// ACQUIRE and ACQUIRE_SHARED are attributes on functions or methods declaring that the function acquires a capability,
// but does not release it. The given capability must not be held on entry, and will be held on exit (exclusively for
// ACQUIRE, shared for ACQUIRE_SHARED).
#define DUCKDB_ACQUIRE(...)        DUCKDB_THREAD_ANNOTATION_ATTRIBUTE(acquire_capability(__VA_ARGS__))
#define DUCKDB_ACQUIRE_SHARED(...) DUCKDB_THREAD_ANNOTATION_ATTRIBUTE(acquire_shared_capability(__VA_ARGS__))

// ACQUIRED_BEFORE and ACQUIRED_AFTER are attributes on member declarations, specifically declarations of mutexes or
// other capabilities. These declarations enforce a particular order in which the mutexes must be acquired, in order
// to prevent deadlock.
#define DUCKDB_ACQUIRED_BEFORE(...) DUCKDB_THREAD_ANNOTATION_ATTRIBUTE(acquired_before(__VA_ARGS__))
#define DUCKDB_ACQUIRED_AFTER(...)  DUCKDB_THREAD_ANNOTATION_ATTRIBUTE(acquired_after(__VA_ARGS__))

// RELEASE, RELEASE_SHARED, and RELEASE_GENERIC declare that the function releases the given capability. The capability
// must be held on entry (exclusively for RELEASE, shared for RELEASE_SHARED, exclusively or shared for
// RELEASE_GENERIC), and will no longer be held on exit.
#define DUCKDB_RELEASE(...)         DUCKDB_THREAD_ANNOTATION_ATTRIBUTE(release_capability(__VA_ARGS__))
#define DUCKDB_RELEASE_SHARED(...)  DUCKDB_THREAD_ANNOTATION_ATTRIBUTE(release_shared_capability(__VA_ARGS__))
#define DUCKDB_RELEASE_GENERIC(...) DUCKDB_THREAD_ANNOTATION_ATTRIBUTE(release_generic_capability(__VA_ARGS__))

// EXCLUDES is an attribute on functions or methods, which declares that the caller must not hold the given
// capabilities. This annotation is used to prevent deadlock. Many mutex implementations are not re-entrant, so
// deadlock can occur if the function acquires the mutex a second time.
#define DUCKDB_EXCLUDES(...) DUCKDB_THREAD_ANNOTATION_ATTRIBUTE(locks_excluded(__VA_ARGS__))

// NO_THREAD_SAFETY_ANALYSIS is an attribute on functions or methods, which turns off thread safety checking for that
// method. It provides an escape hatch for functions which are either (1) deliberately thread-unsafe, or (2) are
// thread-safe, but too complicated for the analysis to understand.
#define DUCKDB_NO_THREAD_SAFETY_ANALYSIS DUCKDB_THREAD_ANNOTATION_ATTRIBUTE(no_thread_safety_analysis)

// RETURN_CAPABILITY is an attribute on functions or methods, which declares that the function returns a reference to
// the given capability. It is used to annotate getter methods that return mutexes.
#define DUCKDB_RETURN_CAPABILITY(x) DUCKDB_THREAD_ANNOTATION_ATTRIBUTE(lock_returned(x))

// These are attributes on a function or method that tries to acquire the given capability, and returns a boolean value
// indicating success or failure. The first argument must be true or false, to specify which return value indicates
// success, and the remaining arguments are interpreted in the same way as DUCKDB_ACQUIRE.
#define DUCKDB_TRY_ACQUIRE(...)        DUCKDB_THREAD_ANNOTATION_ATTRIBUTE(try_acquire_capability(__VA_ARGS__))
#define DUCKDB_TRY_ACQUIRE_SHARED(...) DUCKDB_THREAD_ANNOTATION_ATTRIBUTE(try_acquire_shared_capability(__VA_ARGS__))

// These are attributes on a function or method which asserts the calling thread already holds the given capability,
// for example by performing a run-time test and terminating if the capability is not held. Presence of this annotation
// causes the analysis to assume the capability is held after calls to the annotated function.
#define DUCKDB_ASSERT_CAPABILITY(x)        DUCKDB_THREAD_ANNOTATION_ATTRIBUTE(assert_capability(x))
#define DUCKDB_ASSERT_SHARED_CAPABILITY(x) DUCKDB_THREAD_ANNOTATION_ATTRIBUTE(assert_shared_capability(x))
