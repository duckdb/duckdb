/*-------------------------------------------------------------------------
 *
 * fallback.h
 *    Fallback for platforms without spinlock and/or atomics support. Slower
 *    than native atomics support, but not unusably slow.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/port/atomics/fallback.h
 *
 *-------------------------------------------------------------------------
 */

/* intentionally no include guards, should only be included by atomics.h */
#ifndef INSIDE_ATOMICS_H
#	error "should be included via atomics.h"
#endif

#ifndef pg_memory_barrier_impl
/*
 * If we have no memory barrier implementation for this architecture, we
 * fall back to acquiring and releasing a spinlock.  This might, in turn,
 * fall back to the semaphore-based spinlock implementation, which will be
 * amazingly slow.
 *
 * It's not self-evident that every possible legal implementation of a
 * spinlock acquire-and-release would be equivalent to a full memory barrier.
 * For example, I'm not sure that Itanium's acq and rel add up to a full
 * fence.  But all of our actual implementations seem OK in this regard.
 */
#define PG_HAVE_MEMORY_BARRIER_EMULATION

extern void pg_spinlock_barrier(void);
#define pg_memory_barrier_impl pg_spinlock_barrier
#endif

#ifndef pg_compiler_barrier_impl
/*
 * If the compiler/arch combination does not provide compiler barriers,
 * provide a fallback.  The fallback simply consists of a function call into
 * an externally defined function.  That should guarantee compiler barrier
 * semantics except for compilers that do inter translation unit/global
 * optimization - those better provide an actual compiler barrier.
 *
 * A native compiler barrier for sure is a lot faster than this...
 */
#define PG_HAVE_COMPILER_BARRIER_EMULATION
extern void pg_extern_compiler_barrier(void);
#define pg_compiler_barrier_impl pg_extern_compiler_barrier
#endif


/*
 * If we have atomics implementation for this platform, fall back to providing
 * the atomics API using a spinlock to protect the internal state. Possibly
 * the spinlock implementation uses semaphores internally...
 *
 * We have to be a bit careful here, as it's not guaranteed that atomic
 * variables are mapped to the same address in every process (e.g. dynamic
 * shared memory segments). We can't just hash the address and use that to map
 * to a spinlock. Instead assign a spinlock on initialization of the atomic
 * variable.
 */
#if !defined(PG_HAVE_ATOMIC_FLAG_SUPPORT) && !defined(PG_HAVE_ATOMIC_U32_SUPPORT)

#define PG_HAVE_ATOMIC_FLAG_SIMULATION
#define PG_HAVE_ATOMIC_FLAG_SUPPORT

typedef struct pg_atomic_flag
{
	/*
	 * To avoid circular includes we can't use s_lock as a type here. Instead
	 * just reserve enough space for all spinlock types. Some platforms would
	 * be content with just one byte instead of 4, but that's not too much
	 * waste.
	 */
#if defined(__hppa) || defined(__hppa__)	/* HP PA-RISC, GCC and HP compilers */
	int			sema[4];
#else
	int			sema;
#endif
} pg_atomic_flag;

#endif /* PG_HAVE_ATOMIC_FLAG_SUPPORT */

#if !defined(PG_HAVE_ATOMIC_U32_SUPPORT)

#define PG_HAVE_ATOMIC_U32_SIMULATION

#define PG_HAVE_ATOMIC_U32_SUPPORT
typedef struct pg_atomic_uint32
{
	/* Check pg_atomic_flag's definition above for an explanation */
#if defined(__hppa) || defined(__hppa__)	/* HP PA-RISC, GCC and HP compilers */
	int			sema[4];
#else
	int			sema;
#endif
	volatile uint32 value;
} pg_atomic_uint32;

#endif /* PG_HAVE_ATOMIC_U32_SUPPORT */

#if defined(PG_USE_INLINE) || defined(ATOMICS_INCLUDE_DEFINITIONS)

#ifdef PG_HAVE_ATOMIC_FLAG_SIMULATION

#define PG_HAVE_ATOMIC_INIT_FLAG
extern void pg_atomic_init_flag_impl(volatile pg_atomic_flag *ptr);

#define PG_HAVE_ATOMIC_TEST_SET_FLAG
extern bool pg_atomic_test_set_flag_impl(volatile pg_atomic_flag *ptr);

#define PG_HAVE_ATOMIC_CLEAR_FLAG
extern void pg_atomic_clear_flag_impl(volatile pg_atomic_flag *ptr);

#define PG_HAVE_ATOMIC_UNLOCKED_TEST_FLAG
static inline bool
pg_atomic_unlocked_test_flag_impl(volatile pg_atomic_flag *ptr)
{
	/*
	 * Can't do this efficiently in the semaphore based implementation - we'd
	 * have to try to acquire the semaphore - so always return true. That's
	 * correct, because this is only an unlocked test anyway. Do this in the
	 * header so compilers can optimize the test away.
	 */
	return true;
}

#endif /* PG_HAVE_ATOMIC_FLAG_SIMULATION */

#ifdef PG_HAVE_ATOMIC_U32_SIMULATION

#define PG_HAVE_ATOMIC_INIT_U32
extern void pg_atomic_init_u32_impl(volatile pg_atomic_uint32 *ptr, uint32 val_);

#define PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U32
extern bool pg_atomic_compare_exchange_u32_impl(volatile pg_atomic_uint32 *ptr,
												uint32 *expected, uint32 newval);

#define PG_HAVE_ATOMIC_FETCH_ADD_U32
extern uint32 pg_atomic_fetch_add_u32_impl(volatile pg_atomic_uint32 *ptr, int32 add_);

#endif /* PG_HAVE_ATOMIC_U32_SIMULATION */


#endif /* defined(PG_USE_INLINE) || defined(ATOMICS_INCLUDE_DEFINITIONS) */
