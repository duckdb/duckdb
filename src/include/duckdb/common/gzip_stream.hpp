//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/gzip_stream.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

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
	idx_t data_start = 0;
	void *mz_stream_ptr = nullptr; // void* so we don't have to include the header
	data_ptr_t in_buff = nullptr, in_buff_start, in_buff_end, out_buff = nullptr; // various buffers & pointers
	bool is_initialized = false;
	std::string filename;
	idx_t BUFFER_SIZE = 1024;
};

class GzipStream : public std::istream {
public:
	GzipStream(std::string filename) : std::istream(new GzipStreamBuf(filename)) {
		exceptions(std::ios_base::badbit);
	}
	~GzipStream() {
		if (rdbuf())
			delete rdbuf();
	}
}; // class istream

} // namespace duckdb
