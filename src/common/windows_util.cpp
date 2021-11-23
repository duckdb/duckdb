#include "duckdb/common/windows_util.hpp"

namespace duckdb {

#ifdef DUCKDB_WINDOWS

std::wstring WindowsUtil::WindowsUTF8ToUnicode(const char *input) {
	idx_t result_size;

	result_size = MultiByteToWideChar(CP_UTF8, 0, input, -1, nullptr, 0);
	if (result_size == 0) {
		throw IOException("Failure in MultiByteToWideChar");
	}
	auto buffer = unique_ptr<wchar_t[]>(new wchar_t[result_size]);
	result_size = MultiByteToWideChar(CP_UTF8, 0, input, -1, buffer.get(), result_size);
	if (result_size == 0) {
		throw IOException("Failure in MultiByteToWideChar");
	}
	return std::wstring(buffer.get(), result_size);
}

string WindowsUtil::WindowsUnicodeToUTF8(LPCWSTR input) {
	idx_t result_size;

	result_size = WideCharToMultiByte(CP_UTF8, 0, input, -1, 0, 0, 0, 0);
	if (result_size == 0) {
		throw IOException("Failure in WideCharToMultiByte");
	}
	auto buffer = unique_ptr<char[]>(new char[result_size]);
	result_size = WideCharToMultiByte(CP_UTF8, 0, input, -1, buffer.get(), result_size, 0, 0);
	if (result_size == 0) {
		throw IOException("Failure in WideCharToMultiByte");
	}
	return string(buffer.get(), result_size - 1);
}


#endif

}
