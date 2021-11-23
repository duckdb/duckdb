#include "duckdb/common/windows_util.hpp"

namespace duckdb {

#ifdef DUCKDB_WINDOWS

wstring WindowsUtil::WindowsUTF8ToUnicode(const char *input) {
	idx_t result_size;

	result_size = MultiByteToWideChar(CP_UTF8, 0, input, -1, nullptr, 0);
	if (result_size == 0) {
		throw IOException("Failure in MultiByteToWideChar");
	}
	wstring result;
	result.reserve(result_size);
	result_size = MultiByteToWideChar(CP_UTF8, 0, zText, -1, &result[0], result_size);
	if (result_size == 0) {
		throw IOException("Failure in MultiByteToWideChar");
	}
	return result;
}

wstring WindowsUtil::WindowsUTF8ToUnicode(const string &input) {
	return WindowsUTF8ToUnicode(input.c_str());
}

string WindowsUtil::WindowsUnicodeToUTF8(LPCWSTR input) {
	idx_t result_size;

	result_size = WideCharToMultiByte(CP_UTF8, 0, input, -1, 0, 0, 0, 0);
	if (result_size == 0) {
		throw IOException("Failure in WideCharToMultiByte");
	}
	string result;
	result.reserve(result_size);

	result_size = WideCharToMultiByte(CP_UTF8, 0, input, -1, &result[0], result_size, 0, 0);
	if (result_size == 0) {
		throw IOException("Failure in WideCharToMultiByte");
	}
	return result;
}


#endif

}
