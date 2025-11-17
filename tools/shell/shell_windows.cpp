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
#include "linenoise.hpp"

namespace duckdb_shell {

/*
** Convert a UTF-8 string to Microsoft Unicode.
**
*/
static std::wstring winUtf8ToUnicode(const string &zText) {
	int nChar = MultiByteToWideChar(CP_UTF8, 0, zText.c_str(), zText.size(), NULL, 0);
	if (nChar == 0) {
		return std::wstring();
	}

	std::wstring result(nChar, (WCHAR)0);
	auto zWideText = (LPWSTR)result.data();
	auto conversion_result = MultiByteToWideChar(CP_UTF8, 0, zText.c_str(), zText.size(), zWideText, nChar);
	if (nChar != conversion_result) {
		return std::wstring();
	}
	return result;
}

/*
** Convert a Microsoft Unicode string to UTF-8.
**
*/
static string winUnicodeToUtf8(const std::wstring &zWideText) {
	int nByte = WideCharToMultiByte(CP_UTF8, 0, zWideText.c_str(), zWideText.size(), 0, 0, 0, 0);
	if (nByte == 0) {
		return string();
	}
	string result(nByte, '\0');
	auto zText = (LPSTR)result.data();
	auto conversion_result = WideCharToMultiByte(CP_UTF8, 0, zWideText.c_str(), zWideText.size(), zText, nByte, 0, 0);
	if (conversion_result != nByte) {
		return string();
	}
	return result;
}

/*
** Convert an ANSI string to Microsoft Unicode, using the ANSI or OEM
** code page.
**
*/
static std::wstring winMbcsToUnicode(const string &zText, bool useAnsi) {
	int codepage = useAnsi ? CP_ACP : CP_OEMCP;

	int nChar = MultiByteToWideChar(codepage, 0, zText.c_str(), zText.size(), NULL, 0);
	if (nChar == 0) {
		return std::wstring();
	}
	std::wstring result(nChar, (WCHAR)0);
	auto zMbcsText = (LPWSTR)result.data();
	nChar = MultiByteToWideChar(codepage, 0, zText.c_str(), zText.size(), zMbcsText, nChar);
	if (nChar == 0) {
		return std::wstring();
	}
	return result;
}

/*
** Convert a Microsoft Unicode string to a multi-byte character string,
** using the ANSI or OEM code page.
**
*/
static string winUnicodeToMbcs(const std::wstring &zWideText, bool useAnsi) {
	int codepage = useAnsi ? CP_ACP : CP_OEMCP;

	int nChar = WideCharToMultiByte(codepage, 0, zWideText.c_str(), zWideText.size(), 0, 0, 0, 0);
	if (nChar == 0) {
		return string();
	}
	string result(nChar, '\0');
	auto zMbcsText = (LPSTR)result.data();
	auto conversion_result =
	    WideCharToMultiByte(codepage, 0, zWideText.c_str(), zWideText.size(), zMbcsText, nChar, 0, 0);
	if (nChar != conversion_result) {
		return string();
	}
	return result;
}

/*
** Convert a multi-byte character string to UTF-8.
**
*/
static string winMbcsToUtf8(const string &zText, bool useAnsi) {
	auto zTmpWide = winMbcsToUnicode(zText, useAnsi);
	return winUnicodeToUtf8(zTmpWide);
}

/*
** Convert a UTF-8 string to a multi-byte character string.
**
*/
static string winUtf8ToMbcs(const string &zText, bool useAnsi) {
	auto zTmpWide = winUtf8ToUnicode(zText);
	return winUnicodeToMbcs(zTmpWide, useAnsi);
}

std::wstring ShellState::Win32Utf8ToUnicode(const string &zText) {
	return winUtf8ToUnicode(zText);
}
string ShellState::Win32UnicodeToUtf8(const std::wstring &zWideText) {
	return winUnicodeToUtf8(zWideText);
}
string ShellState::Win32MbcsToUtf8(const string &zText, bool useAnsi) {
	return winMbcsToUtf8(zText, useAnsi);
}
string ShellState::Win32Utf8ToMbcs(const string &zText, bool useAnsi) {
	return winUtf8ToMbcs(zText, useAnsi);
}

} // namespace duckdb_shell

#endif /* SQLITE_OS_WIN */
