# Macros to detect C compiler features
# config/c-compiler.m4


# PGAC_C_SIGNED
# -------------
# Check if the C compiler understands signed types.
AC_DEFUN([PGAC_C_SIGNED],
[AC_CACHE_CHECK(for signed types, pgac_cv_c_signed,
[AC_TRY_COMPILE([],
[signed char c; signed short s; signed int i;],
[pgac_cv_c_signed=yes],
[pgac_cv_c_signed=no])])
if test x"$pgac_cv_c_signed" = xno ; then
  AC_DEFINE(signed,, [Define to empty if the C compiler does not understand signed types.])
fi])# PGAC_C_SIGNED



# PGAC_C_INLINE
# -------------
# Check if the C compiler understands inline functions without being
# noisy about unused static inline functions. Some older compilers
# understand inline functions (as tested by AC_C_INLINE) but warn about
# them if they aren't used in a translation unit.
#
# This test used to just define an inline function, but some compilers
# (notably clang) got too smart and now warn about unused static
# inline functions when defined inside a .c file, but not when defined
# in an included header. Since the latter is what we want to use, test
# to see if the warning appears when the function is in a header file.
# Not pretty, but it works.
#
# Defines: inline, PG_USE_INLINE
AC_DEFUN([PGAC_C_INLINE],
[AC_C_INLINE
AC_CACHE_CHECK([for quiet inline (no complaint if unreferenced)], pgac_cv_c_inline_quietly,
  [pgac_cv_c_inline_quietly=no
  if test "$ac_cv_c_inline" != no; then
    pgac_c_inline_save_werror=$ac_c_werror_flag
    ac_c_werror_flag=yes
    AC_LINK_IFELSE([AC_LANG_PROGRAM([#include "$srcdir/config/test_quiet_include.h"],[])],
                   [pgac_cv_c_inline_quietly=yes])
    ac_c_werror_flag=$pgac_c_inline_save_werror
  fi])
if test "$pgac_cv_c_inline_quietly" != no; then
  AC_DEFINE_UNQUOTED([PG_USE_INLINE], 1,
    [Define to 1 if "static inline" works without unwanted warnings from ]
    [compilations where static inline functions are defined but not called.])
fi
])# PGAC_C_INLINE



# PGAC_TYPE_64BIT_INT(TYPE)
# -------------------------
# Check if TYPE is a working 64 bit integer type. Set HAVE_TYPE_64 to
# yes or no respectively, and define HAVE_TYPE_64 if yes.
AC_DEFUN([PGAC_TYPE_64BIT_INT],
[define([Ac_define], [translit([have_$1_64], [a-z *], [A-Z_P])])dnl
define([Ac_cachevar], [translit([pgac_cv_type_$1_64], [ *], [_p])])dnl
AC_CACHE_CHECK([whether $1 is 64 bits], [Ac_cachevar],
[AC_TRY_RUN(
[typedef $1 ac_int64;

/*
 * These are globals to discourage the compiler from folding all the
 * arithmetic tests down to compile-time constants.
 */
ac_int64 a = 20000001;
ac_int64 b = 40000005;

int does_int64_work()
{
  ac_int64 c,d;

  if (sizeof(ac_int64) != 8)
    return 0;			/* definitely not the right size */

  /* Do perfunctory checks to see if 64-bit arithmetic seems to work */
  c = a * b;
  d = (c + b) / b;
  if (d != a+1)
    return 0;
  return 1;
}
main() {
  exit(! does_int64_work());
}],
[Ac_cachevar=yes],
[Ac_cachevar=no],
[# If cross-compiling, check the size reported by the compiler and
# trust that the arithmetic works.
AC_COMPILE_IFELSE([AC_LANG_BOOL_COMPILE_TRY([], [sizeof($1) == 8])],
                  Ac_cachevar=yes,
                  Ac_cachevar=no)])])

Ac_define=$Ac_cachevar
if test x"$Ac_cachevar" = xyes ; then
  AC_DEFINE(Ac_define, 1, [Define to 1 if `]$1[' works and is 64 bits.])
fi
undefine([Ac_define])dnl
undefine([Ac_cachevar])dnl
])# PGAC_TYPE_64BIT_INT



# PGAC_CHECK_ALIGNOF(TYPE, [INCLUDES = DEFAULT-INCLUDES])
# -----------------------------------------------------
# Find the alignment requirement of the given type. Define the result
# as ALIGNOF_TYPE.  This macro works even when cross compiling.
# (Modelled after AC_CHECK_SIZEOF.)

AC_DEFUN([PGAC_CHECK_ALIGNOF],
[AS_LITERAL_IF([$1], [],
               [AC_FATAL([$0: requires literal arguments])])dnl
AC_CHECK_TYPE([$1], [], [], [$2])
AC_CACHE_CHECK([alignment of $1], [AS_TR_SH([pgac_cv_alignof_$1])],
[if test "$AS_TR_SH([ac_cv_type_$1])" = yes; then
  _AC_COMPUTE_INT([((char*) & pgac_struct.field) - ((char*) & pgac_struct)],
                  [AS_TR_SH([pgac_cv_alignof_$1])],
                  [AC_INCLUDES_DEFAULT([$2])
struct { char filler; $1 field; } pgac_struct;],
                  [AC_MSG_ERROR([cannot compute alignment of $1, 77])])
else
  AS_TR_SH([pgac_cv_alignof_$1])=0
fi])dnl
AC_DEFINE_UNQUOTED(AS_TR_CPP(alignof_$1),
                   [$AS_TR_SH([pgac_cv_alignof_$1])],
                   [The alignment requirement of a `$1'.])
])# PGAC_CHECK_ALIGNOF


# PGAC_C_FUNCNAME_SUPPORT
# -----------------------
# Check if the C compiler understands __func__ (C99) or __FUNCTION__ (gcc).
# Define HAVE_FUNCNAME__FUNC or HAVE_FUNCNAME__FUNCTION accordingly.
AC_DEFUN([PGAC_C_FUNCNAME_SUPPORT],
[AC_CACHE_CHECK(for __func__, pgac_cv_funcname_func_support,
[AC_TRY_COMPILE([#include <stdio.h>],
[printf("%s\n", __func__);],
[pgac_cv_funcname_func_support=yes],
[pgac_cv_funcname_func_support=no])])
if test x"$pgac_cv_funcname_func_support" = xyes ; then
AC_DEFINE(HAVE_FUNCNAME__FUNC, 1,
          [Define to 1 if your compiler understands __func__.])
else
AC_CACHE_CHECK(for __FUNCTION__, pgac_cv_funcname_function_support,
[AC_TRY_COMPILE([#include <stdio.h>],
[printf("%s\n", __FUNCTION__);],
[pgac_cv_funcname_function_support=yes],
[pgac_cv_funcname_function_support=no])])
if test x"$pgac_cv_funcname_function_support" = xyes ; then
AC_DEFINE(HAVE_FUNCNAME__FUNCTION, 1,
          [Define to 1 if your compiler understands __FUNCTION__.])
fi
fi])# PGAC_C_FUNCNAME_SUPPORT



# PGAC_PROG_CC_CFLAGS_OPT
# -----------------------
# Given a string, check if the compiler supports the string as a
# command-line option. If it does, add the string to CFLAGS.
AC_DEFUN([PGAC_PROG_CC_CFLAGS_OPT],
[AC_MSG_CHECKING([if $CC supports $1])
pgac_save_CFLAGS=$CFLAGS
CFLAGS="$pgac_save_CFLAGS $1"
ac_save_c_werror_flag=$ac_c_werror_flag
ac_c_werror_flag=yes
_AC_COMPILE_IFELSE([AC_LANG_PROGRAM()],
                   AC_MSG_RESULT(yes),
                   [CFLAGS="$pgac_save_CFLAGS"
                    AC_MSG_RESULT(no)])
ac_c_werror_flag=$ac_save_c_werror_flag
])# PGAC_PROG_CC_CFLAGS_OPT



# PGAC_PROG_CC_LDFLAGS_OPT
# ------------------------
# Given a string, check if the compiler supports the string as a
# command-line option. If it does, add the string to LDFLAGS.
# For reasons you'd really rather not know about, this checks whether
# you can link to a particular function, not just whether you can link.
# In fact, we must actually check that the resulting program runs :-(
AC_DEFUN([PGAC_PROG_CC_LDFLAGS_OPT],
[AC_MSG_CHECKING([if $CC supports $1])
pgac_save_LDFLAGS=$LDFLAGS
LDFLAGS="$pgac_save_LDFLAGS $1"
AC_RUN_IFELSE([AC_LANG_PROGRAM([extern void $2 (); void (*fptr) () = $2;],[])],
              AC_MSG_RESULT(yes),
              [LDFLAGS="$pgac_save_LDFLAGS"
               AC_MSG_RESULT(no)],
              [LDFLAGS="$pgac_save_LDFLAGS"
               AC_MSG_RESULT(assuming no)])
])# PGAC_PROG_CC_LDFLAGS_OPT



# PGAC_SSE42_CRC32_INTRINSICS
# -----------------------
# Check if the compiler supports the x86 CRC instructions added in SSE 4.2,
# using the _mm_crc32_u8 and _mm_crc32_u32 intrinsic functions. (We don't
# test the 8-byte variant, _mm_crc32_u64, but it is assumed to be present if
# the other ones are, on x86-64 platforms)
#
# An optional compiler flag can be passed as argument (e.g. -msse4.2). If the
# intrinsics are supported, sets pgac_sse42_crc32_intrinsics, and CFLAGS_SSE42.
#
# Copied from upstream.
#
AC_DEFUN([PGAC_SSE42_CRC32_INTRINSICS],
[define([Ac_cachevar], [AS_TR_SH([pgac_cv_sse42_crc32_intrinsics_$1])])dnl
AC_CACHE_CHECK([for _mm_crc32_u8 and _mm_crc32_u32 with CFLAGS=$1], [Ac_cachevar],
[pgac_save_CFLAGS=$CFLAGS
CFLAGS="$pgac_save_CFLAGS $1"
AC_LINK_IFELSE([AC_LANG_PROGRAM([#include <nmmintrin.h>],
  [unsigned int crc = 0;
   crc = _mm_crc32_u8(crc, 0);
   crc = _mm_crc32_u32(crc, 0);
   /* return computed value, to prevent the above being optimized away */
   return crc == 0;])],
  [Ac_cachevar=yes],
  [Ac_cachevar=no])
CFLAGS="$pgac_save_CFLAGS"])
if test x"$Ac_cachevar" = x"yes"; then
  CFLAGS_SSE42="$1"
  pgac_sse42_crc32_intrinsics=yes
fi
undefine([Ac_cachevar])dnl
])# PGAC_SSE42_CRC32_INTRINSICS



# PGAC_HAVE_GCC__SYNC_CHAR_TAS
# -------------------------
# Check if the C compiler understands __sync_lock_test_and_set(char),
# and define HAVE_GCC__SYNC_CHAR_TAS
#
# NB: There are platforms where test_and_set is available but compare_and_swap
# is not, so test this separately.
# NB: Some platforms only do 32bit tas, others only do 8bit tas. Test both.
AC_DEFUN([PGAC_HAVE_GCC__SYNC_CHAR_TAS],
[AC_CACHE_CHECK(for builtin __sync char locking functions, pgac_cv_gcc_sync_char_tas,
[AC_TRY_LINK([],
  [char lock = 0;
   __sync_lock_test_and_set(&lock, 1);
   __sync_lock_release(&lock);],
  [pgac_cv_gcc_sync_char_tas="yes"],
  [pgac_cv_gcc_sync_char_tas="no"])])
if test x"$pgac_cv_gcc_sync_char_tas" = x"yes"; then
  AC_DEFINE(HAVE_GCC__SYNC_CHAR_TAS, 1, [Define to 1 if you have __sync_lock_test_and_set(char *) and friends.])
fi])# PGAC_HAVE_GCC__SYNC_CHAR_TAS

# PGAC_HAVE_GCC__SYNC_INT32_TAS
# -------------------------
# Check if the C compiler understands __sync_lock_test_and_set(),
# and define HAVE_GCC__SYNC_INT32_TAS
AC_DEFUN([PGAC_HAVE_GCC__SYNC_INT32_TAS],
[AC_CACHE_CHECK(for builtin __sync int32 locking functions, pgac_cv_gcc_sync_int32_tas,
[AC_TRY_LINK([],
  [int lock = 0;
   __sync_lock_test_and_set(&lock, 1);
   __sync_lock_release(&lock);],
  [pgac_cv_gcc_sync_int32_tas="yes"],
  [pgac_cv_gcc_sync_int32_tas="no"])])
if test x"$pgac_cv_gcc_sync_int32_tas" = x"yes"; then
  AC_DEFINE(HAVE_GCC__SYNC_INT32_TAS, 1, [Define to 1 if you have __sync_lock_test_and_set(int *) and friends.])
fi])# PGAC_HAVE_GCC__SYNC_INT32_TAS

# PGAC_HAVE_GCC__SYNC_INT32_CAS
# -------------------------
# Check if the C compiler understands __sync_compare_and_swap() for 32bit
# types, and define HAVE_GCC__SYNC_INT32_CAS if so.
AC_DEFUN([PGAC_HAVE_GCC__SYNC_INT32_CAS],
[AC_CACHE_CHECK(for builtin __sync int32 atomic operations, pgac_cv_gcc_sync_int32_cas,
[AC_TRY_LINK([],
  [int val = 0;
   __sync_val_compare_and_swap(&val, 0, 37);],
  [pgac_cv_gcc_sync_int32_cas="yes"],
  [pgac_cv_gcc_sync_int32_cas="no"])])
if test x"$pgac_cv_gcc_sync_int32_cas" = x"yes"; then
  AC_DEFINE(HAVE_GCC__SYNC_INT32_CAS, 1, [Define to 1 if you have __sync_compare_and_swap(int *, int, int).])
fi])# PGAC_HAVE_GCC__SYNC_INT32_CAS

# PGAC_HAVE_GCC__SYNC_INT64_CAS
# -------------------------
# Check if the C compiler understands __sync_compare_and_swap() for 64bit
# types, and define HAVE_GCC__SYNC_INT64_CAS if so.
AC_DEFUN([PGAC_HAVE_GCC__SYNC_INT64_CAS],
[AC_CACHE_CHECK(for builtin __sync int64 atomic operations, pgac_cv_gcc_sync_int64_cas,
[AC_TRY_LINK([],
  [PG_INT64_TYPE lock = 0;
   __sync_val_compare_and_swap(&lock, 0, (PG_INT64_TYPE) 37);],
  [pgac_cv_gcc_sync_int64_cas="yes"],
  [pgac_cv_gcc_sync_int64_cas="no"])])
if test x"$pgac_cv_gcc_sync_int64_cas" = x"yes"; then
  AC_DEFINE(HAVE_GCC__SYNC_INT64_CAS, 1, [Define to 1 if you have __sync_compare_and_swap(int64 *, int64, int64).])
fi])# PGAC_HAVE_GCC__SYNC_INT64_CAS

# PGAC_HAVE_GCC__ATOMIC_INT32_CAS
# -------------------------
# Check if the C compiler understands __atomic_compare_exchange_n() for 32bit
# types, and define HAVE_GCC__ATOMIC_INT32_CAS if so.
AC_DEFUN([PGAC_HAVE_GCC__ATOMIC_INT32_CAS],
[AC_CACHE_CHECK(for builtin __atomic int32 atomic operations, pgac_cv_gcc_atomic_int32_cas,
[AC_TRY_LINK([],
  [int val = 0;
   int expect = 0;
   __atomic_compare_exchange_n(&val, &expect, 37, 0, __ATOMIC_SEQ_CST, __ATOMIC_RELAXED);],
  [pgac_cv_gcc_atomic_int32_cas="yes"],
  [pgac_cv_gcc_atomic_int32_cas="no"])])
if test x"$pgac_cv_gcc_atomic_int32_cas" = x"yes"; then
  AC_DEFINE(HAVE_GCC__ATOMIC_INT32_CAS, 1, [Define to 1 if you have __atomic_compare_exchange_n(int *, int *, int).])
fi])# PGAC_HAVE_GCC__ATOMIC_INT32_CAS

# PGAC_HAVE_GCC__ATOMIC_INT64_CAS
# -------------------------
# Check if the C compiler understands __atomic_compare_exchange_n() for 64bit
# types, and define HAVE_GCC__ATOMIC_INT64_CAS if so.
AC_DEFUN([PGAC_HAVE_GCC__ATOMIC_INT64_CAS],
[AC_CACHE_CHECK(for builtin __atomic int64 atomic operations, pgac_cv_gcc_atomic_int64_cas,
[AC_TRY_LINK([],
  [PG_INT64_TYPE val = 0;
   PG_INT64_TYPE expect = 0;
   __atomic_compare_exchange_n(&val, &expect, 37, 0, __ATOMIC_SEQ_CST, __ATOMIC_RELAXED);],
  [pgac_cv_gcc_atomic_int64_cas="yes"],
  [pgac_cv_gcc_atomic_int64_cas="no"])])
if test x"$pgac_cv_gcc_atomic_int64_cas" = x"yes"; then
  AC_DEFINE(HAVE_GCC__ATOMIC_INT64_CAS, 1, [Define to 1 if you have __atomic_compare_exchange_n(int64 *, int *, int64).])
fi])# PGAC_HAVE_GCC__ATOMIC_INT64_CAS
