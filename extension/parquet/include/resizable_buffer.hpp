//===----------------------------------------------------------------------===//
//                         DuckDB
//
// resizable_buffer.hpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/common/common.hpp"

#include <exception>

namespace duckdb {

class ByteBuffer { // on to the 10 thousandth impl
public:
	char *ptr = nullptr;
	uint64_t len = 0;

	ByteBuffer(){};
	ByteBuffer(char *ptr, uint64_t len) : ptr(ptr), len(len){};

	void inc(uint64_t increment) {
		available(increment);
		len -= increment;
		ptr += increment;
	}

	template <class T> T read() {
		available(sizeof(T));
		T val = Load<T>((data_ptr_t)ptr);
		inc(sizeof(T));
		return val;
	}

	void copy_to(char *dest, uint64_t len) {
		available(len);
		memcpy(dest, ptr, len);
	}

	void available(uint64_t req_len) {
		if (req_len > len) {
			throw std::runtime_error("Out of buffer");
		}
	}
};

class ResizeableBuffer : public ByteBuffer {
public:
	void resize(uint64_t new_size) {
		if (new_size > len) {
			auto new_holder = std::unique_ptr<char[]>(new char[new_size]);
			holder = move(new_holder);
		}
		len = new_size;
		ptr = holder.get();
	}

private:
	std::unique_ptr<char[]> holder = nullptr;
};

} // namespace duckdb
