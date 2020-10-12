/*
** 2004 May 22
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
**
** This file contains code that is specific to Windows.
*/

#if defined(_WIN32) || defined(WIN32)
#include "stripped_sqlite_int.h"
#include <windows.h>
#define SQLITE_OMIT_AUTOINIT

/*
** Allocate and zero memory.
*/
void *sqlite3MallocZero(u64 n) {
	void *p = sqlite3Malloc(n);
	if (p) {
		memset(p, 0, (size_t)n);
	}
	return p;
}

#define osMultiByteToWideChar MultiByteToWideChar

#define osWideCharToMultiByte WideCharToMultiByte

/*
** Convert a UTF-8 string to Microsoft Unicode.
**
** Space to hold the returned string is obtained from sqlite3_malloc().
*/
static LPWSTR winUtf8ToUnicode(const char *zText){
  int nChar;
  LPWSTR zWideText;

  nChar = osMultiByteToWideChar(CP_UTF8, 0, zText, -1, NULL, 0);
  if( nChar==0 ){
    return 0;
  }
  zWideText = (LPWSTR) sqlite3MallocZero( nChar*sizeof(WCHAR) );
  if( zWideText==0 ){
    return 0;
  }
  nChar = osMultiByteToWideChar(CP_UTF8, 0, zText, -1, zWideText,
                                nChar);
  if( nChar==0 ){
    sqlite3_free(zWideText);
    zWideText = 0;
  }
  return zWideText;
}

/*
** Convert a Microsoft Unicode string to UTF-8.
**
** Space to hold the returned string is obtained from sqlite3_malloc().
*/
static char *winUnicodeToUtf8(LPCWSTR zWideText){
  int nByte;
  char *zText;

  nByte = osWideCharToMultiByte(CP_UTF8, 0, zWideText, -1, 0, 0, 0, 0);
  if( nByte == 0 ){
    return 0;
  }
  zText = (char*) sqlite3MallocZero( nByte );
  if( zText==0 ){
    return 0;
  }
  nByte = osWideCharToMultiByte(CP_UTF8, 0, zWideText, -1, zText, nByte,
                                0, 0);
  if( nByte == 0 ){
    sqlite3_free(zText);
    zText = 0;
  }
  return zText;
}

/*
** Convert an ANSI string to Microsoft Unicode, using the ANSI or OEM
** code page.
**
** Space to hold the returned string is obtained from sqlite3_malloc().
*/
static LPWSTR winMbcsToUnicode(const char *zText, int useAnsi){
  int nByte;
  LPWSTR zMbcsText;
  int codepage = useAnsi ? CP_ACP : CP_OEMCP;

  nByte = osMultiByteToWideChar(codepage, 0, zText, -1, NULL,
                                0)*sizeof(WCHAR);
  if( nByte==0 ){
    return 0;
  }
  zMbcsText = (LPWSTR) sqlite3MallocZero( nByte*sizeof(WCHAR) );
  if( zMbcsText==0 ){
    return 0;
  }
  nByte = osMultiByteToWideChar(codepage, 0, zText, -1, zMbcsText,
                                nByte);
  if( nByte==0 ){
    sqlite3_free(zMbcsText);
    zMbcsText = 0;
  }
  return zMbcsText;
}

/*
** Convert a Microsoft Unicode string to a multi-byte character string,
** using the ANSI or OEM code page.
**
** Space to hold the returned string is obtained from sqlite3_malloc().
*/
static char *winUnicodeToMbcs(LPCWSTR zWideText, int useAnsi){
  int nByte;
  char *zText;
  int codepage = useAnsi ? CP_ACP : CP_OEMCP;

  nByte = osWideCharToMultiByte(codepage, 0, zWideText, -1, 0, 0, 0, 0);
  if( nByte == 0 ){
    return 0;
  }
  zText = (char*) sqlite3MallocZero( nByte );
  if( zText==0 ){
    return 0;
  }
  nByte = osWideCharToMultiByte(codepage, 0, zWideText, -1, zText,
                                nByte, 0, 0);
  if( nByte == 0 ){
    sqlite3_free(zText);
    zText = 0;
  }
  return zText;
}

/*
** Convert a multi-byte character string to UTF-8.
**
** Space to hold the returned string is obtained from sqlite3_malloc().
*/
static char *winMbcsToUtf8(const char *zText, int useAnsi){
  char *zTextUtf8;
  LPWSTR zTmpWide;

  zTmpWide = winMbcsToUnicode(zText, useAnsi);
  if( zTmpWide==0 ){
    return 0;
  }
  zTextUtf8 = winUnicodeToUtf8(zTmpWide);
  sqlite3_free(zTmpWide);
  return zTextUtf8;
}

/*
** Convert a UTF-8 string to a multi-byte character string.
**
** Space to hold the returned string is obtained from sqlite3_malloc().
*/
static char *winUtf8ToMbcs(const char *zText, int useAnsi){
  char *zTextMbcs;
  LPWSTR zTmpWide;

  zTmpWide = winUtf8ToUnicode(zText);
  if( zTmpWide==0 ){
    return 0;
  }
  zTextMbcs = winUnicodeToMbcs(zTmpWide, useAnsi);
  sqlite3_free(zTmpWide);
  return zTextMbcs;
}

/*
** This is a public wrapper for the winUtf8ToUnicode() function.
*/
LPWSTR sqlite3_win32_utf8_to_unicode(const char *zText){
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !zText ){
    (void)SQLITE_MISUSE_BKPT;
    return 0;
  }
#endif
#ifndef SQLITE_OMIT_AUTOINIT
  if( sqlite3_initialize() ) return 0;
#endif
  return winUtf8ToUnicode(zText);
}

/*
** This is a public wrapper for the winUnicodeToUtf8() function.
*/
char *sqlite3_win32_unicode_to_utf8(LPCWSTR zWideText){
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !zWideText ){
    (void)SQLITE_MISUSE_BKPT;
    return 0;
  }
#endif
#ifndef SQLITE_OMIT_AUTOINIT
  if( sqlite3_initialize() ) return 0;
#endif
  return winUnicodeToUtf8(zWideText);
}

/*
** This is a public wrapper for the winMbcsToUtf8() function.
*/
char *sqlite3_win32_mbcs_to_utf8_v2(const char *zText, int useAnsi){
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !zText ){
    (void)SQLITE_MISUSE_BKPT;
    return 0;
  }
#endif
#ifndef SQLITE_OMIT_AUTOINIT
  if( sqlite3_initialize() ) return 0;
#endif
  return winMbcsToUtf8(zText, useAnsi);
}

/*
** This is a public wrapper for the winUtf8ToMbcs() function.
*/
char *sqlite3_win32_utf8_to_mbcs_v2(const char *zText, int useAnsi){
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !zText ){
    (void)SQLITE_MISUSE_BKPT;
    return 0;
  }
#endif
#ifndef SQLITE_OMIT_AUTOINIT
  if( sqlite3_initialize() ) return 0;
#endif
  return winUtf8ToMbcs(zText, useAnsi);
}

#endif /* SQLITE_OS_WIN */
