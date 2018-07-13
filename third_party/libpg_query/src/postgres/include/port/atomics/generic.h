/*-------------------------------------------------------------------------
 *
 * generic.h
 *	  Implement higher level operations based on some lower level tomic
 *	  operations.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/port/atomics/generic.h
 *
 *-------------------------------------------------------------------------
 */

/* intentionally no include guards, should only be included by atomics.h */
#ifndef INSIDE_ATOMICS_H
#	error "should be included via atomics.h"
#endif

/*
 * If read or write barriers are undefined, we upgrade them to full memory
 * barriers.
 */
#if !defined(pg_read_barrier_impl)
#	define pg_read_barrier_impl pg_memory_barrier_impl
#endif
#if !defined(pg_write_barrier_impl)
#	define pg_write_barrier_impl pg_memory_barrier_impl
#endif

#ifndef PG_HAVE_SPIN_DELAY
#define PG_HAVE_SPIN_DELAY
#define pg_spin_delay_impl()	((void)0)
#endif


/* provide fallback */
#if !defined(PG_HAVE_ATOMIC_FLAG_SUPPORT) && defined(PG_HAVE_ATOMIC_U32_SUPPORT)
#define PG_HAVE_ATOMIC_FLAG_SUPPORT
typedef pg_atomic_uint32 pg_atomic_flag;
#endif

#if defined(PG_USE_INLINE) || defined(ATOMICS_INCLUDE_DEFINITIONS)

#ifndef PG_HAVE_ATOMIC_READ_U32
#define PG_HAVE_ATOMIC_READ_U32
static inline uint32
pg_atomic_read_u32_impl(volatile pg_atomic_uint32 *ptr)
{
	return *(&ptr->value);
}
#endif

#ifndef PG_HAVE_ATOMIC_WRITE_U32
#define PG_HAVE_ATOMIC_WRITE_U32
static inline void
pg_atomic_write_u32_impl(volatile pg_atomic_uint32 *ptr, uint32 val)
{
	ptr->value = val;
}
#endif

/*
 * provide fallback for test_and_set using atomic_exchange if available
 */
#if !defined(PG_HAVE_ATOMIC_TEST_SET_FLAG) && defined(PG_HAVE_ATOMIC_EXCHANGE_U32)

#define PG_HAVE_ATOMIC_INIT_FLAG
static inline void
pg_atomic_init_flag_impl(volatile pg_atomic_flag *ptr)
{
	pg_atomic_write_u32_impl(ptr, 0);
}

#define PG_HAVE_ATOMIC_TEST_SET_FLAG
static inline bool
pg_atomic_test_set_flag_impl(volatile pg_atomic_flag *ptr)
{
	return pg_atomic_exchange_u32_impl(ptr, &value, 1) == 0;
}

#define PG_HAVE_ATOMIC_UNLOCKED_TEST_FLAG
static inline bool
pg_atomic_unlocked_test_flag_impl(volatile pg_atomic_flag *ptr)
{
	return pg_atomic_read_u32_impl(ptr) == 0;
}


#define PG_HAVE_ATOMIC_CLEAR_FLAG
static inline void
pg_atomic_clear_flag_impl(volatile pg_atomic_flag *ptr)
{
	/* XXX: release semantics suffice? */
	pg_memory_barrier_impl();
	pg_atomic_write_u32_impl(ptr, 0);
}

/*
 * provide fallback for test_and_set using atomic_compare_exchange if
 * available.
 */
#elif !defined(PG_HAVE_ATOMIC_TEST_SET_FLAG) && defined(PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U32)

#define PG_HAVE_ATOMIC_INIT_FLAG
static inline void
pg_atomic_init_flag_impl(volatile pg_atomic_flag *ptr)
{
	pg_atomic_write_u32_impl(ptr, 0);
}

#define PG_HAVE_ATOMIC_TEST_SET_FLAG
static inline bool
pg_atomic_test_set_flag_impl(volatile pg_atomic_flag *ptr)
{
	uint32 value = 0;
	return pg_atomic_compare_exchange_u32_impl(ptr, &value, 1);
}

#define PG_HAVE_ATOMIC_UNLOCKED_TEST_FLAG
static inline bool
pg_atomic_unlocked_test_flag_impl(volatile pg_atomic_flag *ptr)
{
	return pg_atomic_read_u32_impl(ptr) == 0;
}

#define PG_HAVE_ATOMIC_CLEAR_FLAG
static inline void
pg_atomic_clear_flag_impl(volatile pg_atomic_flag *ptr)
{
	/*
	 * Use a memory barrier + plain write if we have a native memory
	 * barrier. But don't do so if memory barriers use spinlocks - that'd lead
	 * to circularity if flags are used to implement spinlocks.
	 */
#ifndef PG_HAVE_MEMORY_BARRIER_EMULATION
	/* XXX: release semantics suffice? */
	pg_memory_barrier_impl();
	pg_atomic_write_u32_impl(ptr, 0);
#else
	uint32 value = 1;
	pg_atomic_compare_exchange_u32_impl(ptr, &value, 0);
#endif
}

#elif !defined(PG_HAVE_ATOMIC_TEST_SET_FLAG)
#	error "No pg_atomic_test_and_set provided"
#endif /* !defined(PG_HAVE_ATOMIC_TEST_SET_FLAG) */


#ifndef PG_HAVE_ATOMIC_INIT_U32
#define PG_HAVE_ATOMIC_INIT_U32
static inline void
pg_atomic_init_u32_impl(volatile pg_atomic_uint32 *ptr, uint32 val_)
{
	pg_atomic_write_u32_impl(ptr, val_);
}
#endif

#if !defined(PG_HAVE_ATOMIC_EXCHANGE_U32) && defined(PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U32)
#define PG_HAVE_ATOMIC_EXCHANGE_U32
static inline uint32
pg_atomic_exchange_u32_impl(volatile pg_atomic_uint32 *ptr, uint32 xchg_)
{
	uint32 old;
	while (true)
	{
		old = pg_atomic_read_u32_impl(ptr);
		if (pg_atomic_compare_exchange_u32_impl(ptr, &old, xchg_))
			break;
	}
	return old;
}
#endif

#if !defined(PG_HAVE_ATOMIC_FETCH_ADD_U32) && defined(PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U32)
#define PG_HAVE_ATOMIC_FETCH_ADD_U32
static inline uint32
pg_atomic_fetch_add_u32_impl(volatile pg_atomic_uint32 *ptr, int32 add_)
{
	uint32 old;
	while (true)
	{
		old = pg_atomic_read_u32_impl(ptr);
		if (pg_atomic_compare_exchange_u32_impl(ptr, &old, old + add_))
			break;
	}
	return old;
}
#endif

#if !defined(PG_HAVE_ATOMIC_FETCH_SUB_U32) && defined(PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U32)
#define PG_HAVE_ATOMIC_FETCH_SUB_U32
static inline uint32
pg_atomic_fetch_sub_u32_impl(volatile pg_atomic_uint32 *ptr, int32 sub_)
{
	return pg_atomic_fetch_add_u32_impl(ptr, -sub_);
}
#endif

#if !defined(PG_HAVE_ATOMIC_FETCH_AND_U32) && defined(PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U32)
#define PG_HAVE_ATOMIC_FETCH_AND_U32
static inline uint32
pg_atomic_fetch_and_u32_impl(volatile pg_atomic_uint32 *ptr, uint32 and_)
{
	uint32 old;
	while (true)
	{
		old = pg_atomic_read_u32_impl(ptr);
		if (pg_atomic_compare_exchange_u32_impl(ptr, &old, old & and_))
			break;
	}
	return old;
}
#endif

#if !defined(PG_HAVE_ATOMIC_FETCH_OR_U32) && defined(PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U32)
#define PG_HAVE_ATOMIC_FETCH_OR_U32
static inline uint32
pg_atomic_fetch_or_u32_impl(volatile pg_atomic_uint32 *ptr, uint32 or_)
{
	uint32 old;
	while (true)
	{
		old = pg_atomic_read_u32_impl(ptr);
		if (pg_atomic_compare_exchange_u32_impl(ptr, &old, old | or_))
			break;
	}
	return old;
}
#endif

#if !defined(PG_HAVE_ATOMIC_ADD_FETCH_U32) && defined(PG_HAVE_ATOMIC_FETCH_ADD_U32)
#define PG_HAVE_ATOMIC_ADD_FETCH_U32
static inline uint32
pg_atomic_add_fetch_u32_impl(volatile pg_atomic_uint32 *ptr, int32 add_)
{
	return pg_atomic_fetch_add_u32_impl(ptr, add_) + add_;
}
#endif

#if !defined(PG_HAVE_ATOMIC_SUB_FETCH_U32) && defined(PG_HAVE_ATOMIC_FETCH_SUB_U32)
#define PG_HAVE_ATOMIC_SUB_FETCH_U32
static inline uint32
pg_atomic_sub_fetch_u32_impl(volatile pg_atomic_uint32 *ptr, int32 sub_)
{
	return pg_atomic_fetch_sub_u32_impl(ptr, sub_) - sub_;
}
#endif

#ifdef PG_HAVE_ATOMIC_U64_SUPPORT

#if !defined(PG_HAVE_ATOMIC_EXCHANGE_U64) && defined(PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U64)
#define PG_HAVE_ATOMIC_EXCHANGE_U64
static inline uint64
pg_atomic_exchange_u64_impl(volatile pg_atomic_uint64 *ptr, uint64 xchg_)
{
	uint64 old;
	while (true)
	{
		old = ptr->value;
		if (pg_atomic_compare_exchange_u64_impl(ptr, &old, xchg_))
			break;
	}
	return old;
}
#endif

#ifndef PG_HAVE_ATOMIC_WRITE_U64
#define PG_HAVE_ATOMIC_WRITE_U64
static inline void
pg_atomic_write_u64_impl(volatile pg_atomic_uint64 *ptr, uint64 val)
{
	/*
	 * 64 bit writes aren't safe on all platforms. In the generic
	 * implementation implement them as an atomic exchange.
	 */
	pg_atomic_exchange_u64_impl(ptr, val);
}
#endif

#ifndef PG_HAVE_ATOMIC_READ_U64
#define PG_HAVE_ATOMIC_READ_U64
static inline uint64
pg_atomic_read_u64_impl(volatile pg_atomic_uint64 *ptr)
{
	uint64 old = 0;

	/*
	 * 64 bit reads aren't safe on all platforms. In the generic
	 * implementation implement them as a compare/exchange with 0. That'll
	 * fail or succeed, but always return the old value. Possible might store
	 * a 0, but only if the prev. value also was a 0 - i.e. harmless.
	 */
	pg_atomic_compare_exchange_u64_impl(ptr, &old, 0);

	return old;
}
#endif

#ifndef PG_HAVE_ATOMIC_INIT_U64
#define PG_HAVE_ATOMIC_INIT_U64
static inline void
pg_atomic_init_u64_impl(volatile pg_atomic_uint64 *ptr, uint64 val_)
{
	pg_atomic_write_u64_impl(ptr, val_);
}
#endif

#if !defined(PG_HAVE_ATOMIC_FETCH_ADD_U64) && defined(PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U64)
#define PG_HAVE_ATOMIC_FETCH_ADD_U64
static inline uint64
pg_atomic_fetch_add_u64_impl(volatile pg_atomic_uint64 *ptr, int64 add_)
{
	uint64 old;
	while (true)
	{
		old = pg_atomic_read_u64_impl(ptr);
		if (pg_atomic_compare_exchange_u64_impl(ptr, &old, old + add_))
			break;
	}
	return old;
}
#endif

#if !defined(PG_HAVE_ATOMIC_FETCH_SUB_U64) && defined(PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U64)
#define PG_HAVE_ATOMIC_FETCH_SUB_U64
static inline uint64
pg_atomic_fetch_sub_u64_impl(volatile pg_atomic_uint64 *ptr, int64 sub_)
{
	return pg_atomic_fetch_add_u64_impl(ptr, -sub_);
}
#endif

#if !defined(PG_HAVE_ATOMIC_FETCH_AND_U64) && defined(PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U64)
#define PG_HAVE_ATOMIC_FETCH_AND_U64
static inline uint64
pg_atomic_fetch_and_u64_impl(volatile pg_atomic_uint64 *ptr, uint64 and_)
{
	uint64 old;
	while (true)
	{
		old = pg_atomic_read_u64_impl(ptr);
		if (pg_atomic_compare_exchange_u64_impl(ptr, &old, old & and_))
			break;
	}
	return old;
}
#endif

#if !defined(PG_HAVE_ATOMIC_FETCH_OR_U64) && defined(PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U64)
#define PG_HAVE_ATOMIC_FETCH_OR_U64
static inline uint64
pg_atomic_fetch_or_u64_impl(volatile pg_atomic_uint64 *ptr, uint64 or_)
{
	uint64 old;
	while (true)
	{
		old = pg_atomic_read_u64_impl(ptr);
		if (pg_atomic_compare_exchange_u64_impl(ptr, &old, old | or_))
			break;
	}
	return old;
}
#endif

#if !defined(PG_HAVE_ATOMIC_ADD_FETCH_U64) && defined(PG_HAVE_ATOMIC_FETCH_ADD_U64)
#define PG_HAVE_ATOMIC_ADD_FETCH_U64
static inline uint64
pg_atomic_add_fetch_u64_impl(volatile pg_atomic_uint64 *ptr, int64 add_)
{
	return pg_atomic_fetch_add_u64_impl(ptr, add_) + add_;
}
#endif

#if !defined(PG_HAVE_ATOMIC_SUB_FETCH_U64) && defined(PG_HAVE_ATOMIC_FETCH_SUB_U64)
#define PG_HAVE_ATOMIC_SUB_FETCH_U64
static inline uint64
pg_atomic_sub_fetch_u64_impl(volatile pg_atomic_uint64 *ptr, int64 sub_)
{
	return pg_atomic_fetch_sub_u64_impl(ptr, sub_) - sub_;
}
#endif

#endif /* PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U64 */

#endif /* defined(PG_USE_INLINE) || defined(ATOMICS_INCLUDE_DEFINITIONS) */
