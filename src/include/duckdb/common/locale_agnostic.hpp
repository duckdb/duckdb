#pragma once

#include <locale>

namespace duckdb {

template <class STREAM, class... ARGS>
STREAM GetLocalAgnostic(ARGS... args) {
	STREAM stream(args...);
	stream.imbue(std::locale::classic());
	return stream;
}

} // namespace duckdb
