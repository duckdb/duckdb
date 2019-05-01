//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/gzip_stream.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <fstream>
#include <sstream>

namespace duckdb {

class GzipStreamBuf : public std::streambuf {
public:
	GzipStreamBuf(std::string filename) : filename(filename) {
	}

	GzipStreamBuf(const GzipStreamBuf &) = delete;
	GzipStreamBuf(GzipStreamBuf &&) = default;
	GzipStreamBuf &operator=(const GzipStreamBuf &) = delete;
	GzipStreamBuf &operator=(GzipStreamBuf &&) = default;

	~GzipStreamBuf();

	std::streambuf::int_type underflow() override;

private:
	void initialize();
	std::fstream input;
	size_t data_start;
	void *mz_stream_ptr = nullptr;                                    // void* so we don't have to include the header
	char *in_buff = nullptr, *in_buff_start, *in_buff_end, *out_buff = nullptr; // various buffers & pointers
	bool is_initialized = false;
	std::string filename;
};

class GzipStream : public std::istream {
public:
	GzipStream(std::string filename) : std::istream(new GzipStreamBuf(filename)) {
		exceptions(std::ios_base::badbit);
	}
}; // class istream

} // namespace duckdb
