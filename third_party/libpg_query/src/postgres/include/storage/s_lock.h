/*-------------------------------------------------------------------------
 *
 * s_lock.h
 *	   Hardware-dependent implementation of spinlocks.
 *
 *	NOTE: none of the macros in this file are intended to be called directly.
 *	Call them through the hardware-independent macros in spin.h.
 *
 *	The following hardware-dependent macros must be provided for each
 *	supported platform:
 *
 *	void S_INIT_LOCK(slock_t *lock)
 *		Initialize a spinlock (to the unlocked state).
 *
 *	int S_LOCK(slock_t *lock)
 *		Acquire a spinlock, waiting if necessary.
 *		Time out and abort() if unable to acquire the lock in a
 *		"reasonable" amount of time --- typically ~ 1 minute.
 *		Should return number of "delays"; see s_lock.c
 *
 *	void S_UNLOCK(slock_t *lock)
 *		Unlock a previously acquired lock.
 *
 *	bool S_LOCK_FREE(slock_t *lock)
 *		Tests if the lock is free. Returns TRUE if free, FALSE if locked.
 *		This does *not* change the state of the lock.
 *
 *	void SPIN_DELAY(void)
 *		Delay operation to occur inside spinlock wait loop.
 *
 *	Note to implementors: there are default implementations for all these
 *	macros at the bottom of the file.  Check if your platform can use
 *	these or needs to override them.
 *
 *  Usually, S_LOCK() is implemented in terms of even lower-level macros
 *	TAS() and TAS_SPIN():
 *
 *	int TAS(slock_t *lock)
 *		Atomic test-and-set instruction.  Attempt to acquire the lock,
 *		but do *not* wait.	Returns 0 if successful, nonzero if unable
 *		to acquire the lock.
 *
 *	int TAS_SPIN(slock_t *lock)
 *		Like TAS(), but this version is used when waiting for a lock
 *		previously found to be contended.  By default, this is the
 *		same as TAS(), but on some architectures it's better to poll a
 *		contended lock using an unlocked instruction and retry the
 *		atomic test-and-set only when it appears free.
 *
 *	TAS() and TAS_SPIN() are NOT part of the API, and should never be called
 *	directly.
 *
 *	CAUTION: on some platforms TAS() and/or TAS_SPIN() may sometimes report
 *	failure to acquire a lock even when the lock is not locked.  For example,
 *	on Alpha TAS() will "fail" if interrupted.  Therefore a retry loop must
 *	always be used, even if you are certain the lock is free.
 *
 *	It is the responsibility of these macros to make sure that the compiler
 *	does not re-order accesses to shared memory to precede the actual lock
 *	acquisition, or follow the lock release.  Prior to PostgreSQL 9.5, this
 *	was the caller's responsibility, which meant that callers had to use
 *	volatile-qualified pointers to refer to both the spinlock itself and the
 *	shared data being accessed within the spinlocked critical section.  This
 *	was notationally awkward, easy to forget (and thus error-prone), and
 *	prevented some useful compiler optimizations.  For these reasons, we
 *	now require that the macros themselves prevent compiler re-ordering,
 *	so that the caller doesn't need to take special precautions.
 *
 *	On platforms with weak memory ordering, the TAS(), TAS_SPIN(), and
 *	S_UNLOCK() macros must further include hardware-level memory fence
 *	instructions to prevent similar re-ordering at the hardware level.
 *	TAS() and TAS_SPIN() must guarantee that loads and stores issued after
 *	the macro are not executed until the lock has been obtained.  Conversely,
 *	S_UNLOCK() must guarantee that loads and stores issued before the macro
 *	have been executed before the lock is released.
 *
 *	On most supported platforms, TAS() uses a tas() function written
 *	in assembly language to execute a hardware atomic-test-and-set
 *	instruction.  Equivalent OS-supplied mutex routines could be used too.
 *
 *	If no system-specific TAS() is available (ie, HAVE_SPINLOCKS is not
 *	defined), then we fall back on an emulation that uses SysV semaphores
 *	(see spin.c).  This emulation will be MUCH MUCH slower than a proper TAS()
 *	implementation, because of the cost of a kernel call per lock or unlock.
 *	An old report is that Postgres spends around 40% of its time in semop(2)
 *	when using the SysV semaphore code.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *	  src/include/storage/s_lock.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef S_LOCK_H
#define S_LOCK_H

#ifdef HAVE_SPINLOCKS	/* skip spinlocks if requested */

#if defined(__GNUC__) || defined(__INTEL_COMPILER)
/*************************************************************************
 * All the gcc inlines
 * Gcc consistently defines the CPU as __cpu__.
 * Other compilers use __cpu or __cpu__ so we test for both in those cases.
 */

/*----------
 * Standard gcc asm format (assuming "volatile slock_t *lock"):

	__asm__ __volatile__(
		"	instruction	\n"
		"	instruction	\n"
		"	instruction	\n"
:		"=r"(_res), "+m"(*lock)		// return register, in/out lock value
:		"r"(lock)					// lock pointer, in input register
:		"memory", "cc");			// show clobbered registers here

 * The output-operands list (after first colon) should always include
 * "+m"(*lock), whether or not the asm code actually refers to this
 * operand directly.  This ensures that gcc believes the value in the
 * lock variable is used and set by the asm code.  Also, the clobbers
 * list (after third colon) should always include "memory"; this prevents
 * gcc from thinking it can cache the values of shared-memory fields
 * across the asm code.  Add "cc" if your asm code changes the condition
 * code register, and also list any temp registers the code uses.
 *----------
 */


#ifdef __i386__		/* 32-bit i386 */
#define HAS_TEST_AND_SET

typedef unsigned char slock_t;

#define TAS(lock) tas(lock)

static __inline__ int
tas(volatile slock_t *lock)
{
	register slock_t _res = 1;

	/*
	 * Use a non-locking test before asserting the bus lock.  Note that the
	 * extra test appears to be a small loss on some x86 platforms and a small
	 * win on others; it's by no means clear that we should keep it.
	 *
	 * When this was last tested, we didn't have separate TAS() and TAS_SPIN()
	 * macros.  Nowadays it probably would be better to do a non-locking test
	 * in TAS_SPIN() but not in TAS(), like on x86_64, but no-one's done the
	 * testing to verify that.  Without some empirical evidence, better to
	 * leave it alone.
	 */
	__asm__ __volatile__(
		"	cmpb	$0,%1	\n"
		"	jne		1f		\n"
		"	lock			\n"
		"	xchgb	%0,%1	\n"
		"1: \n"
:		"+q"(_res), "+m"(*lock)
:		/* no inputs */
:		"memory", "cc");
	return (int) _res;
}

#define SPIN_DELAY() spin_delay()

static __inline__ void
spin_delay(void)
{
	/*
	 * This sequence is equivalent to the PAUSE instruction ("rep" is
	 * ignored by old IA32 processors if the following instruction is
	 * not a string operation); the IA-32 Architecture Software
	 * Developer's Manual, Vol. 3, Section 7.7.2 describes why using
	 * PAUSE in the inner loop of a spin lock is necessary for good
	 * performance:
	 *
	 *     The PAUSE instruction improves the performance of IA-32
	 *     processors supporting Hyper-Threading Technology when
	 *     executing spin-wait loops and other routines where one
	 *     thread is accessing a shared lock or semaphore in a tight
	 *     polling loop. When executing a spin-wait loop, the
	 *     processor can suffer a severe performance penalty when
	 *     exiting the loop because it detects a possible memory order
	 *     violation and flushes the core processor's pipeline. The
	 *     PAUSE instruction provides a hint to the processor that the
	 *     code sequence is a spin-wait loop. The processor uses this
	 *     hint to avoid the memory order violation and prevent the
	 *     pipeline flush. In addition, the PAUSE instruction
	 *     de-pipelines the spin-wait loop to prevent it from
	 *     consuming execution resources excessively.
	 */
	__asm__ __volatile__(
		" rep; nop			\n");
}

#endif	 /* __i386__ */


#ifdef __x86_64__		/* AMD Opteron, Intel EM64T */
#define HAS_TEST_AND_SET

typedef unsigned char slock_t;

#define TAS(lock) tas(lock)

/*
 * On Intel EM64T, it's a win to use a non-locking test before the xchg proper,
 * but only when spinning.
 *
 * See also Implementing Scalable Atomic Locks for Multi-Core Intel(tm) EM64T
 * and IA32, by Michael Chynoweth and Mary R. Lee. As of this writing, it is
 * available at:
 * http://software.intel.com/en-us/articles/implementing-scalable-atomic-locks-for-multi-core-intel-em64t-and-ia32-architectures
 */
#define TAS_SPIN(lock)    (*(lock) ? 1 : TAS(lock))

static __inline__ int
tas(volatile slock_t *lock)
{
	register slock_t _res = 1;

	__asm__ __volatile__(
		"	lock			\n"
		"	xchgb	%0,%1	\n"
:		"+q"(_res), "+m"(*lock)
:		/* no inputs */
:		"memory", "cc");
	return (int) _res;
}

#define SPIN_DELAY() spin_delay()

static __inline__ void
spin_delay(void)
{
	/*
	 * Adding a PAUSE in the spin delay loop is demonstrably a no-op on
	 * Opteron, but it may be of some use on EM64T, so we keep it.
	 */
	__asm__ __volatile__(
		" rep; nop			\n");
}

#endif	 /* __x86_64__ */


#if defined(__ia64__) || defined(__ia64)
/*
 * Intel Itanium, gcc or Intel's compiler.
 *
 * Itanium has weak memory ordering, but we rely on the compiler to enforce
 * strict ordering of accesses to volatile data.  In particular, while the
 * xchg instruction implicitly acts as a memory barrier with 'acquire'
 * semantics, we do not have an explicit memory fence instruction in the
 * S_UNLOCK macro.  We use a regular assignment to clear the spinlock, and
 * trust that the compiler marks the generated store instruction with the
 * ".rel" opcode.
 *
 * Testing shows that assumption to hold on gcc, although I could not find
 * any explicit statement on that in the gcc manual.  In Intel's compiler,
 * the -m[no-]serialize-volatile option controls that, and testing shows that
 * it is enabled by default.
 */
#define HAS_TEST_AND_SET

typedef unsigned int slock_t;

#define TAS(lock) tas(lock)

/* On IA64, it's a win to use a non-locking test before the xchg proper */
#define TAS_SPIN(lock)	(*(lock) ? 1 : TAS(lock))

#ifndef __INTEL_COMPILER

static __inline__ int
tas(volatile slock_t *lock)
{
	long int	ret;

	__asm__ __volatile__(
		"	xchg4 	%0=%1,%2	\n"
:		"=r"(ret), "+m"(*lock)
:		"r"(1)
:		"memory");
	return (int) ret;
}

#else /* __INTEL_COMPILER */

static __inline__ int
tas(volatile slock_t *lock)
{
	int		ret;

	ret = _InterlockedExchange(lock,1);	/* this is a xchg asm macro */

	return ret;
}

#endif /* __INTEL_COMPILER */
#endif	 /* __ia64__ || __ia64 */

/*
 * On ARM and ARM64, we use __sync_lock_test_and_set(int *, int) if available.
 *
 * We use the int-width variant of the builtin because it works on more chips
 * than other widths.
 */
#if defined(__arm__) || defined(__arm) || defined(__aarch64__) || defined(__aarch64)
#ifdef HAVE_GCC__SYNC_INT32_TAS
#define HAS_TEST_AND_SET

#define TAS(lock) tas(lock)

typedef int slock_t;

static __inline__ int
tas(volatile slock_t *lock)
{
	return __sync_lock_test_and_set(lock, 1);
}

#define S_UNLOCK(lock) __sync_lock_release(lock)

#endif	 /* HAVE_GCC__SYNC_INT32_TAS */
#endif	 /* __arm__ || __arm || __aarch64__ || __aarch64 */


/* S/390 and S/390x Linux (32- and 64-bit zSeries) */
#if defined(__s390__) || defined(__s390x__)
#define HAS_TEST_AND_SET

typedef unsigned int slock_t;

#define TAS(lock)	   tas(lock)

static __inline__ int
tas(volatile slock_t *lock)
{
	int			_res = 0;

	__asm__	__volatile__(
		"	cs 	%0,%3,0(%2)		\n"
:		"+d"(_res), "+m"(*lock)
:		"a"(lock), "d"(1)
:		"memory", "cc");
	return _res;
}

#endif	 /* __s390__ || __s390x__ */


#if defined(__sparc__)		/* Sparc */
/*
 * Solaris has always run sparc processors in TSO (total store) mode, but
 * linux didn't use to and the *BSDs still don't. So, be careful about
 * acquire/release semantics. The CPU will treat superfluous membars as
 * NOPs, so it's just code space.
 */
#define HAS_TEST_AND_SET

typedef unsigned char slock_t;

#define TAS(lock) tas(lock)

static __inline__ int
tas(volatile slock_t *lock)
{
	register slock_t _res;

	/*
	 *	See comment in /pg/backend/port/tas/solaris_sparc.s for why this
	 *	uses "ldstub", and that file uses "cas".  gcc currently generates
	 *	sparcv7-targeted binaries, so "cas" use isn't possible.
	 */
	__asm__ __volatile__(
		"	ldstub	[%2], %0	\n"
:		"=r"(_res), "+m"(*lock)
:		"r"(lock)
:		"memory");
#if defined(__sparcv7) || defined(__sparc_v7__)
	/*
	 * No stbar or membar available, luckily no actually produced hardware
	 * requires a barrier.
	 */
#elif defined(__sparcv8) || defined(__sparc_v8__)
	/* stbar is available (and required for both PSO, RMO), membar isn't */
	__asm__ __volatile__ ("stbar	 \n":::"memory");
#else
	/*
	 * #LoadStore (RMO) | #LoadLoad (RMO) together are the appropriate acquire
	 * barrier for sparcv8+ upwards.
	 */
	__asm__ __volatile__ ("membar #LoadStore | #LoadLoad \n":::"memory");
#endif
	return (int) _res;
}

#if defined(__sparcv7) || defined(__sparc_v7__)
/*
 * No stbar or membar available, luckily no actually produced hardware
 * requires a barrier.  We fall through to the default gcc definition of
 * S_UNLOCK in this case.
 */
#elif defined(__sparcv8) || defined(__sparc_v8__)
/* stbar is available (and required for both PSO, RMO), membar isn't */
#define S_UNLOCK(lock)	\
do \
{ \
	__asm__ __volatile__ ("stbar	 \n":::"memory"); \
	*((volatile slock_t *) (lock)) = 0; \
} while (0)
#else
/*
 * #LoadStore (RMO) | #StoreStore (RMO, PSO) together are the appropriate
 * release barrier for sparcv8+ upwards.
 */
#define S_UNLOCK(lock)	\
do \
{ \
	__asm__ __volatile__ ("membar #LoadStore | #StoreStore \n":::"memory"); \
	*((volatile slock_t *) (lock)) = 0; \
} while (0)
#endif

#endif	 /* __sparc__ */


/* PowerPC */
#if defined(__ppc__) || defined(__powerpc__) || defined(__ppc64__) || defined(__powerpc64__)
#define HAS_TEST_AND_SET

typedef unsigned int slock_t;

#define TAS(lock) tas(lock)

/* On PPC, it's a win to use a non-locking test before the lwarx */
#define TAS_SPIN(lock)	(*(lock) ? 1 : TAS(lock))

/*
 * NOTE: per the Enhanced PowerPC Architecture manual, v1.0 dated 7-May-2002,
 * an isync is a sufficient synchronization barrier after a lwarx/stwcx loop.
 * On newer machines, we can use lwsync instead for better performance.
 *
 * Ordinarily, we'd code the branches here using GNU-style local symbols, that
 * is "1f" referencing "1:" and so on.  But some people run gcc on AIX with
 * IBM's assembler as backend, and IBM's assembler doesn't do local symbols.
 * So hand-code the branch offsets; fortunately, all PPC instructions are
 * exactly 4 bytes each, so it's not too hard to count.
 */
static __inline__ int
tas(volatile slock_t *lock)
{
	slock_t _t;
	int _res;

	__asm__ __volatile__(
#ifdef USE_PPC_LWARX_MUTEX_HINT
"	lwarx   %0,0,%3,1	\n"
#else
"	lwarx   %0,0,%3		\n"
#endif
"	cmpwi   %0,0		\n"
"	bne     $+16		\n"		/* branch to li %1,1 */
"	addi    %0,%0,1		\n"
"	stwcx.  %0,0,%3		\n"
"	beq     $+12		\n"		/* branch to lwsync/isync */
"	li      %1,1		\n"
"	b       $+12		\n"		/* branch to end of asm sequence */
#ifdef USE_PPC_LWSYNC
"	lwsync				\n"
#else
"	isync				\n"
#endif
"	li      %1,0		\n"

:	"=&r"(_t), "=r"(_res), "+m"(*lock)
:	"r"(lock)
:	"memory", "cc");
	return _res;
}

/*
 * PowerPC S_UNLOCK is almost standard but requires a "sync" instruction.
 * On newer machines, we can use lwsync instead for better performance.
 */
#ifdef USE_PPC_LWSYNC
#define S_UNLOCK(lock)	\
do \
{ \
	__asm__ __volatile__ ("	lwsync \n" ::: "memory"); \
	*((volatile slock_t *) (lock)) = 0; \
} while (0)
#else
#define S_UNLOCK(lock)	\
do \
{ \
	__asm__ __volatile__ ("	sync \n" ::: "memory"); \
	*((volatile slock_t *) (lock)) = 0; \
} while (0)
#endif /* USE_PPC_LWSYNC */

#endif /* powerpc */


/* Linux Motorola 68k */
#if (defined(__mc68000__) || defined(__m68k__)) && defined(__linux__)
#define HAS_TEST_AND_SET

typedef unsigned char slock_t;

#define TAS(lock) tas(lock)

static __inline__ int
tas(volatile slock_t *lock)
{
	register int rv;

	__asm__	__volatile__(
		"	clrl	%0		\n"
		"	tas		%1		\n"
		"	sne		%0		\n"
:		"=d"(rv), "+m"(*lock)
:		/* no inputs */
:		"memory", "cc");
	return rv;
}

#endif	 /* (__mc68000__ || __m68k__) && __linux__ */


/*
 * VAXen -- even multiprocessor ones
 * (thanks to Tom Ivar Helbekkmo)
 */
#if defined(__vax__)
#define HAS_TEST_AND_SET

typedef unsigned char slock_t;

#define TAS(lock) tas(lock)

static __inline__ int
tas(volatile slock_t *lock)
{
	register int	_res;

	__asm__ __volatile__(
		"	movl 	$1, %0			\n"
		"	bbssi	$0, (%2), 1f	\n"
		"	clrl	%0				\n"
		"1: \n"
:		"=&r"(_res), "+m"(*lock)
:		"r"(lock)
:		"memory");
	return _res;
}

#endif	 /* __vax__ */


#if defined(__mips__) && !defined(__sgi)	/* non-SGI MIPS */
/* Note: on SGI we use the OS' mutex ABI, see below */
/* Note: R10000 processors require a separate SYNC */
#define HAS_TEST_AND_SET

typedef unsigned int slock_t;

#define TAS(lock) tas(lock)

static __inline__ int
tas(volatile slock_t *lock)
{
	register volatile slock_t *_l = lock;
	register int _res;
	register int _tmp;

	__asm__ __volatile__(
		"       .set push           \n"
		"       .set mips2          \n"
		"       .set noreorder      \n"
		"       .set nomacro        \n"
		"       ll      %0, %2      \n"
		"       or      %1, %0, 1   \n"
		"       sc      %1, %2      \n"
		"       xori    %1, 1       \n"
		"       or      %0, %0, %1  \n"
		"       sync                \n"
		"       .set pop              "
:		"=&r" (_res), "=&r" (_tmp), "+R" (*_l)
:		/* no inputs */
:		"memory");
	return _res;
}

/* MIPS S_UNLOCK is almost standard but requires a "sync" instruction */
#define S_UNLOCK(lock)	\
do \
{ \
	__asm__ __volatile__( \
		"       .set push           \n" \
		"       .set mips2          \n" \
		"       .set noreorder      \n" \
		"       .set nomacro        \n" \
		"       sync                \n" \
		"       .set pop              " \
:		/* no outputs */ \
:		/* no inputs */	\
:		"memory"); \
	*((volatile slock_t *) (lock)) = 0; \
} while (0)

#endif /* __mips__ && !__sgi */


#if defined(__m32r__) && defined(HAVE_SYS_TAS_H)	/* Renesas' M32R */
#define HAS_TEST_AND_SET

#include <sys/tas.h>

typedef int slock_t;

#define TAS(lock) tas(lock)

#endif /* __m32r__ */


#if defined(__sh__)				/* Renesas' SuperH */
#define HAS_TEST_AND_SET

typedef unsigned char slock_t;

#define TAS(lock) tas(lock)

static __inline__ int
tas(volatile slock_t *lock)
{
	register int _res;

	/*
	 * This asm is coded as if %0 could be any register, but actually SuperH
	 * restricts the target of xor-immediate to be R0.  That's handled by
	 * the "z" constraint on _res.
	 */
	__asm__ __volatile__(
		"	tas.b @%2    \n"
		"	movt  %0     \n"
		"	xor   #1,%0  \n"
:		"=z"(_res), "+m"(*lock)
:		"r"(lock)
:		"memory", "t");
	return _res;
}

#endif	 /* __sh__ */


/* These live in s_lock.c, but only for gcc */


#if defined(__m68k__) && !defined(__linux__)	/* non-Linux Motorola 68k */
#define HAS_TEST_AND_SET

typedef unsigned char slock_t;
#endif

/*
 * Note that this implementation is unsafe for any platform that can speculate
 * a memory access (either load or store) after a following store.  That
 * happens not to be possible x86 and most legacy architectures (some are
 * single-processor!), but many modern systems have weaker memory ordering.
 * Those that do must define their own version S_UNLOCK() rather than relying
 * on this one.
 */
#if !defined(S_UNLOCK)
#if defined(__INTEL_COMPILER)
#define S_UNLOCK(lock)	\
	do { __memory_barrier(); *(lock) = 0; } while (0)
#else
#define S_UNLOCK(lock)	\
	do { __asm__ __volatile__("" : : : "memory");  *(lock) = 0; } while (0)
#endif
#endif

#endif	/* defined(__GNUC__) || defined(__INTEL_COMPILER) */



/*
 * ---------------------------------------------------------------------
 * Platforms that use non-gcc inline assembly:
 * ---------------------------------------------------------------------
 */

#if !defined(HAS_TEST_AND_SET)	/* We didn't trigger above, let's try here */


#if defined(USE_UNIVEL_CC)		/* Unixware compiler */
#define HAS_TEST_AND_SET

typedef unsigned char slock_t;

#define TAS(lock)	tas(lock)

asm int
tas(volatile slock_t *s_lock)
{
/* UNIVEL wants %mem in column 1, so we don't pg_indent this file */
%mem s_lock
	pushl %ebx
	movl s_lock, %ebx
	movl $255, %eax
	lock
	xchgb %al, (%ebx)
	popl %ebx
}

#endif	 /* defined(USE_UNIVEL_CC) */


#if defined(__hppa) || defined(__hppa__)	/* HP PA-RISC, GCC and HP compilers */
/*
 * HP's PA-RISC
 *
 * See src/backend/port/hpux/tas.c.template for details about LDCWX.  Because
 * LDCWX requires a 16-byte-aligned address, we declare slock_t as a 16-byte
 * struct.  The active word in the struct is whichever has the aligned address;
 * the other three words just sit at -1.
 *
 * When using gcc, we can inline the required assembly code.
 */
#define HAS_TEST_AND_SET

typedef struct
{
	int			sema[4];
} slock_t;

#define TAS_ACTIVE_WORD(lock)	((volatile int *) (((uintptr_t) (lock) + 15) & ~15))

#if defined(__GNUC__)

static __inline__ int
tas(volatile slock_t *lock)
{
	volatile int *lockword = TAS_ACTIVE_WORD(lock);
	register int lockval;

	__asm__ __volatile__(
		"	ldcwx	0(0,%2),%0	\n"
:		"=r"(lockval), "+m"(*lockword)
:		"r"(lockword)
:		"memory");
	return (lockval == 0);
}

/*
 * The hppa implementation doesn't follow the rules of this files and provides
 * a gcc specific implementation outside of the above defined(__GNUC__). It
 * does so to avoid duplication between the HP compiler and gcc. So undefine
 * the generic fallback S_UNLOCK from above.
 */
#ifdef S_UNLOCK
#undef S_UNLOCK
#endif
#define S_UNLOCK(lock)	\
	do { \
		__asm__ __volatile__("" : : : "memory"); \
		*TAS_ACTIVE_WORD(lock) = -1; \
	} while (0)

#endif /* __GNUC__ */

#define S_INIT_LOCK(lock) \
	do { \
		volatile slock_t *lock_ = (lock); \
		lock_->sema[0] = -1; \
		lock_->sema[1] = -1; \
		lock_->sema[2] = -1; \
		lock_->sema[3] = -1; \
	} while (0)

#define S_LOCK_FREE(lock)	(*TAS_ACTIVE_WORD(lock) != 0)

#endif	 /* __hppa || __hppa__ */


#if defined(__hpux) && defined(__ia64) && !defined(__GNUC__)
/*
 * HP-UX on Itanium, non-gcc compiler
 *
 * We assume that the compiler enforces strict ordering of loads/stores on
 * volatile data (see comments on the gcc-version earlier in this file).
 * Note that this assumption does *not* hold if you use the
 * +Ovolatile=__unordered option on the HP-UX compiler, so don't do that.
 *
 * See also Implementing Spinlocks on the Intel Itanium Architecture and
 * PA-RISC, by Tor Ekqvist and David Graves, for more information.  As of
 * this writing, version 1.0 of the manual is available at:
 * http://h21007.www2.hp.com/portal/download/files/unprot/itanium/spinlocks.pdf
 */
#define HAS_TEST_AND_SET

typedef unsigned int slock_t;

#include <ia64/sys/inline.h>
#define TAS(lock) _Asm_xchg(_SZ_W, lock, 1, _LDHINT_NONE)
/* On IA64, it's a win to use a non-locking test before the xchg proper */
#define TAS_SPIN(lock)	(*(lock) ? 1 : TAS(lock))
#define S_UNLOCK(lock)	\
	do { _Asm_mf(); (*(lock)) = 0; } while (0)

#endif	/* HPUX on IA64, non gcc */

#if defined(_AIX)	/* AIX */
/*
 * AIX (POWER)
 */
#define HAS_TEST_AND_SET

#include <sys/atomic_op.h>

typedef int slock_t;

#define TAS(lock)			_check_lock((slock_t *) (lock), 0, 1)
#define S_UNLOCK(lock)		_clear_lock((slock_t *) (lock), 0)
#endif	 /* _AIX */


/* These are in sunstudio_(sparc|x86).s */

#if defined(__SUNPRO_C) && (defined(__i386) || defined(__x86_64__) || defined(__sparc__) || defined(__sparc))
#define HAS_TEST_AND_SET

#if defined(__i386) || defined(__x86_64__) || defined(__sparcv9) || defined(__sparcv8plus)
typedef unsigned int slock_t;
#else
typedef unsigned char slock_t;
#endif

extern slock_t pg_atomic_cas(volatile slock_t *lock, slock_t with,
									  slock_t cmp);

#define TAS(a) (pg_atomic_cas((a), 1, 0) != 0)
#endif


#ifdef WIN32_ONLY_COMPILER
typedef LONG slock_t;

#define HAS_TEST_AND_SET
#define TAS(lock) (InterlockedCompareExchange(lock, 1, 0))

#define SPIN_DELAY() spin_delay()

/* If using Visual C++ on Win64, inline assembly is unavailable.
 * Use a _mm_pause instrinsic instead of rep nop.
 */
#if defined(_WIN64)
static __forceinline void
spin_delay(void)
{
	_mm_pause();
}
#else
static __forceinline void
spin_delay(void)
{
	/* See comment for gcc code. Same code, MASM syntax */
	__asm rep nop;
}
#endif

#include <intrin.h>
#pragma intrinsic(_ReadWriteBarrier)

#define S_UNLOCK(lock)	\
	do { _ReadWriteBarrier(); (*(lock)) = 0; } while (0)

#endif


#endif	/* !defined(HAS_TEST_AND_SET) */


/* Blow up if we didn't have any way to do spinlocks */
#ifndef HAS_TEST_AND_SET
#error PostgreSQL does not have native spinlock support on this platform.  To continue the compilation, rerun configure using --disable-spinlocks.  However, performance will be poor.  Please report this to pgsql-bugs@postgresql.org.
#endif


#else	/* !HAVE_SPINLOCKS */


/*
 * Fake spinlock implementation using semaphores --- slow and prone
 * to fall foul of kernel limits on number of semaphores, so don't use this
 * unless you must!  The subroutines appear in spin.c.
 */
typedef int slock_t;

extern bool s_lock_free_sema(volatile slock_t *lock);
extern void s_unlock_sema(volatile slock_t *lock);
extern void s_init_lock_sema(volatile slock_t *lock, bool nested);
extern int	tas_sema(volatile slock_t *lock);

#define S_LOCK_FREE(lock)	s_lock_free_sema(lock)
#define S_UNLOCK(lock)	 s_unlock_sema(lock)
#define S_INIT_LOCK(lock)	s_init_lock_sema(lock, false)
#define TAS(lock)	tas_sema(lock)


#endif	/* HAVE_SPINLOCKS */


/*
 * Default Definitions - override these above as needed.
 */

#if !defined(S_LOCK)
#define S_LOCK(lock) \
	(TAS(lock) ? s_lock((lock), __FILE__, __LINE__) : 0)
#endif	 /* S_LOCK */

#if !defined(S_LOCK_FREE)
#define S_LOCK_FREE(lock)	(*(lock) == 0)
#endif	 /* S_LOCK_FREE */

#if !defined(S_UNLOCK)
/*
 * Our default implementation of S_UNLOCK is essentially *(lock) = 0.  This
 * is unsafe if the platform can speculate a memory access (either load or
 * store) after a following store; platforms where this is possible must
 * define their own S_UNLOCK.  But CPU reordering is not the only concern:
 * if we simply defined S_UNLOCK() as an inline macro, the compiler might
 * reorder instructions from inside the critical section to occur after the
 * lock release.  Since the compiler probably can't know what the external
 * function s_unlock is doing, putting the same logic there should be adequate.
 * A sufficiently-smart globally optimizing compiler could break that
 * assumption, though, and the cost of a function call for every spinlock
 * release may hurt performance significantly, so we use this implementation
 * only for platforms where we don't know of a suitable intrinsic.  For the
 * most part, those are relatively obscure platform/compiler combinations to
 * which the PostgreSQL project does not have access.
 */
#define USE_DEFAULT_S_UNLOCK
extern void s_unlock(volatile slock_t *lock);
#define S_UNLOCK(lock)		s_unlock(lock)
#endif	 /* S_UNLOCK */

#if !defined(S_INIT_LOCK)
#define S_INIT_LOCK(lock)	S_UNLOCK(lock)
#endif	 /* S_INIT_LOCK */

#if !defined(SPIN_DELAY)
#define SPIN_DELAY()	((void) 0)
#endif	 /* SPIN_DELAY */

#if !defined(TAS)
extern int	tas(volatile slock_t *lock);		/* in port/.../tas.s, or
												 * s_lock.c */

#define TAS(lock)		tas(lock)
#endif	 /* TAS */

#if !defined(TAS_SPIN)
#define TAS_SPIN(lock)	TAS(lock)
#endif	 /* TAS_SPIN */

extern slock_t dummy_spinlock;

/*
 * Platform-independent out-of-line support routines
 */
extern int s_lock(volatile slock_t *lock, const char *file, int line);

/* Support for dynamic adjustment of spins_per_delay */
#define DEFAULT_SPINS_PER_DELAY  100

extern void set_spins_per_delay(int shared_spins_per_delay);
extern int	update_spins_per_delay(int shared_spins_per_delay);

#endif	 /* S_LOCK_H */
