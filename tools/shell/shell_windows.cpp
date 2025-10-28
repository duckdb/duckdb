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
#include <windows.h>
#include "shell_state.hpp"

#define osMultiByteToWideChar MultiByteToWideChar

#define osWideCharToMultiByte WideCharToMultiByte

namespace duckdb_shell {

unique_ptr<uint8_t[]> allocZero(uint64_t byteCount) {
  auto data = unique_ptr<uint8_t[]>(new uint8_t[byteCount]);
  memset(data.get(), 0, byteCount);
  return data;
}

/*
** Convert a UTF-8 string to Microsoft Unicode.
**
*/
static unique_ptr<uint8_t[]> winUtf8ToUnicode(const char *zText){
  int nChar;

  nChar = osMultiByteToWideChar(CP_UTF8, 0, zText, -1, NULL, 0);
  if( nChar==0 ){
    return 0;
  }

  auto data = allocZero(nChar * sizeof(WCHAR));
  auto zWideText = (LPWSTR) data.get();
  nChar = osMultiByteToWideChar(CP_UTF8, 0, zText, -1, zWideText,
                                nChar);
  if( nChar==0 ){
    zWideText = 0;
  }
  return data;
}

/*
** Convert a Microsoft Unicode string to UTF-8.
**
*/
static string winUnicodeToUtf8(LPCWSTR zWideText){
  int nByte;

  nByte = osWideCharToMultiByte(CP_UTF8, 0, zWideText, -1, 0, 0, 0, 0);
  if( nByte == 0 ){
    return 0;
  }
  string result(nByte, '\0');
  auto zText = (LPSTR) result.data();
  nByte = osWideCharToMultiByte(CP_UTF8, 0, zWideText, -1, zText, nByte,
                                0, 0);
  return result;
}

/*
** Convert an ANSI string to Microsoft Unicode, using the ANSI or OEM
** code page.
**
*/
static unique_ptr<uint8_t[]> winMbcsToUnicode(const char *zText, bool useAnsi){
  int nByte;
  int codepage = useAnsi ? CP_ACP : CP_OEMCP;

  nByte = osMultiByteToWideChar(codepage, 0, zText, -1, NULL,
                                0)*sizeof(WCHAR);
  if( nByte==0 ){
    return nullptr;
  }
  auto data = allocZero(nByte*sizeof(WCHAR));
  auto zMbcsText = (LPWSTR) data.get();
  nByte = osMultiByteToWideChar(codepage, 0, zText, -1, zMbcsText,
                                nByte);
  if( nByte==0 ){
    return nullptr;
  }
  return data;
}

/*
** Convert a Microsoft Unicode string to a multi-byte character string,
** using the ANSI or OEM code page.
**
*/
static unique_ptr<uint8_t[]> winUnicodeToMbcs(LPCWSTR zWideText, bool useAnsi){
  int nByte;
  int codepage = useAnsi ? CP_ACP : CP_OEMCP;

  nByte = osWideCharToMultiByte(codepage, 0, zWideText, -1, 0, 0, 0, 0);
  if( nByte == 0 ){
    return nullptr;
  }
  auto data = allocZero(nByte);
  auto zText = (char *) data.get();
  nByte = osWideCharToMultiByte(codepage, 0, zWideText, -1, zText,
                                nByte, 0, 0);
  if( nByte == 0 ){
    return nullptr;
  }
  return data;
}

/*
** Convert a multi-byte character string to UTF-8.
**
*/
static string winMbcsToUtf8(const char *zText, bool useAnsi){

  auto zTmpWide = winMbcsToUnicode(zText, useAnsi);
  if( !zTmpWide ){
    return 0;
  }
  return winUnicodeToUtf8((LPCWSTR) zTmpWide.get());
}

/*
** Convert a UTF-8 string to a multi-byte character string.
**
*/
static unique_ptr<uint8_t[]> winUtf8ToMbcs(const char *zText, bool useAnsi){
  auto zTmpWide = winUtf8ToUnicode(zText);
  if( !zTmpWide ){
    return 0;
  }
  return winUnicodeToMbcs((LPCWSTR) zTmpWide.get(), useAnsi);
}

unique_ptr<uint8_t[]> ShellState::Win32Utf8ToUnicode(const char *zText) {
  return winUtf8ToUnicode(zText);
}
string ShellState::Win32UnicodeToUtf8(void * zWideText) {
  return winUnicodeToUtf8((LPCWSTR) zWideText);
}
string ShellState::Win32MbcsToUtf8(const char *zText, bool useAnsi) {
  return winMbcsToUtf8(zText, useAnsi);
}
unique_ptr<uint8_t[]> ShellState::Win32Utf8ToMbcs(const char *zText, bool useAnsi) {
  return winUtf8ToMbcs(zText, useAnsi);
}

}

#endif /* SQLITE_OS_WIN */
