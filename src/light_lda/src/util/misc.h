/*
 * Copyright (c) 2011, Dongsheng Song <songdongsheng@live.cn>
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _MISC_H_
#define _MISC_H_    1

/**
 * @file misc.h
 * @brief Implementation-related Miscellaneous Definitions and Code
 */

/**
 * @defgroup impl Implementation-related Definitions and Code
 * @ingroup libpthread
 * @{
 */

#ifdef _MSC_VER
#include <intrin.h>
#pragma intrinsic(_InterlockedCompareExchange, _InterlockedDecrement, _InterlockedIncrement, _mm_pause)

#ifdef _WIN64
#pragma intrinsic(_InterlockedCompareExchangePointer)
#endif
#endif

/* Number of 100ns-seconds between the beginning of the Windows epoch
 * (Jan. 1, 1601) and the Unix epoch (Jan. 1, 1970)
 */
#define DELTA_EPOCH_IN_100NS    INT64_C(116444736000000000)

#define MAX_SLEEP_IN_MS         4294967294UL

#define POW10_2     INT64_C(100)
#define POW10_3     INT64_C(1000)
#define POW10_4     INT64_C(10000)
#define POW10_6     INT64_C(1000000)
#define POW10_7     INT64_C(10000000)
#define POW10_9     INT64_C(1000000000)

static __inline void lc_assert(char *message, char *file, unsigned int line)
{
    fprintf(stderr, "Assertion failed: %s , file %s, line %u\n", message, file, line);
    exit(1);
}

#define assert(_Expression) (void)( (!!(_Expression)) || (lc_assert(#_Expression, __FILE__, __LINE__), 0) )

#ifndef _MSC_VER
__attribute__((always_inline))
#endif
static __inline int lc_set_errno(int result)
{
    if (result != 0) {
        errno = result;
        return -1;
    }
    return 0;
}

#ifndef _MSC_VER
__attribute__((always_inline))
#endif
static __inline __int64 FileTimeToUnixTimeIn100NS(FILETIME *input)
{
    return (((__int64) input->dwHighDateTime) << 32 | input->dwLowDateTime) - DELTA_EPOCH_IN_100NS;
}

/* Return milli-seconds since the Unix epoch (jan. 1, 1970) UTC */
static __inline __int64 arch_time_in_ms(void)
{
    FILETIME time;

    GetSystemTimeAsFileTime(&time);
    return FileTimeToUnixTimeIn100NS(&time) / POW10_4;
}

/* Return micro-seconds since the Unix epoch (jan. 1, 1970) UTC */
static __inline void arch_time_in_timespec(struct timespec *ts)
{
    __int64 t;
    FILETIME time;

    GetSystemTimeAsFileTime(&time);
    t = FileTimeToUnixTimeIn100NS(&time);
    ts->tv_sec = t / POW10_7;
    ts->tv_nsec= ((int) (t % POW10_7)) * POW10_2;
}

#ifndef _MSC_VER
__attribute__((always_inline))
#endif
static __inline __int64 arch_time_in_ms_from_timespec(const struct timespec *ts)
{
    return ts->tv_sec * POW10_3 + ts->tv_nsec / POW10_6;
}

static __inline unsigned arch_rel_time_in_ms(const struct timespec *ts)
{
    __int64 t1 = arch_time_in_ms_from_timespec(ts);
    __int64 t2 = arch_time_in_ms();
    __int64 t = t1 - t2;

    if (t < 0 || t >= INT64_C(4294967295))
        return 0;

    return (unsigned) t;
}

static __inline int sched_priority_to_os_priority(int priority)
{
    /* THREAD_PRIORITY_TIME_CRITICAL (15) */
    /* THREAD_PRIORITY_HIGHEST (12, 13, 14) */
    /* THREAD_PRIORITY_ABOVE_NORMAL (9, 10, 11) */
    /* THREAD_PRIORITY_NORMAL (8) */
    /* THREAD_PRIORITY_BELOW_NORMAL (5, 6, 7) */
    /* THREAD_PRIORITY_LOWEST (2, 3, 4) */
    /* THREAD_PRIORITY_IDLE (1) */

    if (priority >= 15)
        return THREAD_PRIORITY_TIME_CRITICAL;
    else if (priority >= 12)
        return THREAD_PRIORITY_HIGHEST;
    else if (priority >= 9)
        return THREAD_PRIORITY_ABOVE_NORMAL;
    else if (priority >= 8)
        return THREAD_PRIORITY_NORMAL;
    else if (priority >= 5)
        return THREAD_PRIORITY_BELOW_NORMAL;
    else if (priority >= 2)
        return THREAD_PRIORITY_LOWEST;
    else
        return THREAD_PRIORITY_IDLE;
}

static __inline int os_priority_to_sched_priority(int os_priority)
{
    int priority = 8;
    switch(os_priority) {
        case THREAD_PRIORITY_TIME_CRITICAL:
            priority = 15;
            break;

        case THREAD_PRIORITY_HIGHEST:
            priority = 13;
            break;

        case THREAD_PRIORITY_ABOVE_NORMAL:
            priority = 10;
            break;

        case THREAD_PRIORITY_NORMAL:
            priority = 8;
            break;

        case THREAD_PRIORITY_BELOW_NORMAL:
            priority = 6;
            break;

        case THREAD_PRIORITY_LOWEST:
            priority = 3;
            break;

        case THREAD_PRIORITY_IDLE:
            priority = 1;
            break;
    }

    return priority;
}

/*
 * http://gcc.gnu.org/onlinedocs/gcc/Machine-Constraints.html
 * http://gcc.gnu.org/onlinedocs/gcc/Atomic-Builtins.html
 * http://www.ibiblio.org/gferg/ldp/GCC-Inline-Assembly-HOWTO.html
 * http://msdn.microsoft.com/en-us/library/7kcdt6fy.aspx [x64 Software Conventions, RAX, (RCX, RDX, R8, R9), R10, R11]
 * http://msdn.microsoft.com/zh-cn/library/26td21ds.aspx [Compiler Intrinsics]
 */

#ifndef _MSC_VER
__attribute__((always_inline))
#endif
static __inline void memory_barrier(void)
{
#ifdef _MSC_VER
    MemoryBarrier();
    /* __faststorefence() */
    /* __asm lock or dword ptr [rsp], 0 */
#else
    __sync_synchronize();
    /* asm volatile("lock orl $0x0,(%%esp)" ::: "memory"); */
#endif
}

#ifndef _MSC_VER
__attribute__((always_inline))
#endif
static __inline void cpu_relax(void)
{
#ifdef _MSC_VER
    YieldProcessor();
    /* _mm_pause(); */
    /* __asm rep nop */
#else
    /* __builtin_ia32_pause(); */
    asm volatile("rep; nop" ::: "memory");
#endif
}

#ifndef _MSC_VER
__attribute__((always_inline))
#endif
static __inline void atomic_set(long volatile *__ptr, long value)
{
    *__ptr = value;
}

#ifndef _MSC_VER
__attribute__((always_inline))
#endif
static __inline long atomic_read(long volatile *__ptr)
{
    return *__ptr;
}

#ifndef _MSC_VER
__attribute__((always_inline))
#endif
static __inline long atomic_fetch_and_add(long volatile *__ptr, long value)
{
#ifdef _MSC_VER
    return _InterlockedExchangeAdd(__ptr, value);
#else
    return __sync_fetch_and_add(__ptr, value);
#endif
}

#ifndef _MSC_VER
__attribute__((always_inline))
#endif
static __inline long atomic_cmpxchg(long volatile *__ptr, long __new, long __old)
{
#ifdef _MSC_VER
    return _InterlockedCompareExchange(__ptr, __new, __old);
#else
    return __sync_val_compare_and_swap (__ptr, __old, __new);
/*
    long prev;
    asm volatile("lock ; cmpxchgl %2, %1" : "=a" (prev), "+m" (*__ptr) : "q" (__new), "0" (__old) : "memory");
    return prev;
 */
#endif
}

#ifndef _MSC_VER
__attribute__((always_inline))
#endif
static __inline void *atomic_cmpxchg_ptr(void * volatile *__ptr, void *__new, void *__old)
{
#ifdef _MSC_VER
#ifdef _WIN64
    return _InterlockedCompareExchangePointer(__ptr, __new, __old);
#else
    return (void *) _InterlockedCompareExchange((volatile long *) __ptr, (long) __new, (long) __old);
#endif
#else
    return __sync_val_compare_and_swap (__ptr, __old, __new);
/*
  void *prev;
#ifdef _WIN64
    asm volatile("lock ; cmpxchgq %2, %1" : "=a" (prev),"+m" (*__ptr) : "q" (__new), "0" (__old) : "memory");
#else
    asm volatile("lock ; cmpxchgl %2, %1" : "=a" (prev),"+m" (*__ptr) : "q" (__new), "0" (__old) : "memory");
#endif
  return prev;
 */
#endif
}

static __inline int get_ncpu()
{
    int n = 0;
    DWORD_PTR pm, sm;

    if (GetProcessAffinityMask(GetCurrentProcess(), &pm, &sm)) {
        while(pm > 0) {
            n += pm & 1;
            pm >>= 1;
        }
    }

    return n > 0 ? n : 1;
}

/** @} */

#endif
