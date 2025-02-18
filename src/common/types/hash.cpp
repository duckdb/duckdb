#include "duckdb/common/types/hash.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/fast_mem.hpp"

#include <functional>
#include <cmath>

namespace duckdb {

template <>
hash_t Hash(uint64_t val) {
	return MurmurHash64(val);
}

template <>
hash_t Hash(int64_t val) {
	return MurmurHash64((uint64_t)val);
}

template <>
hash_t Hash(hugeint_t val) {
	return MurmurHash64(val.lower) ^ MurmurHash64(static_cast<uint64_t>(val.upper));
}

template <>
hash_t Hash(uhugeint_t val) {
	return MurmurHash64(val.lower) ^ MurmurHash64(val.upper);
}

template <class T>
struct FloatingPointEqualityTransform {
	static void OP(T &val) {
		if (val == (T)0.0) {
			// Turn negative zero into positive zero
			val = (T)0.0;
		} else if (std::isnan(val)) {
			val = std::numeric_limits<T>::quiet_NaN();
		}
	}
};

template <>
hash_t Hash(float val) {
	static_assert(sizeof(float) == sizeof(uint32_t), "");
	FloatingPointEqualityTransform<float>::OP(val);
	uint32_t uval = Load<uint32_t>(const_data_ptr_cast(&val));
	return MurmurHash64(uval);
}

template <>
hash_t Hash(double val) {
	static_assert(sizeof(double) == sizeof(uint64_t), "");
	FloatingPointEqualityTransform<double>::OP(val);
	uint64_t uval = Load<uint64_t>(const_data_ptr_cast(&val));
	return MurmurHash64(uval);
}

template <>
hash_t Hash(interval_t val) {
	int64_t months, days, micros;
	val.Normalize(months, days, micros);
	return Hash(days) ^ Hash(months) ^ Hash(micros);
}

template <>
hash_t Hash(dtime_tz_t val) {
	return Hash(val.bits);
}

template <>
hash_t Hash(const char *str) {
	return Hash(str, strlen(str));
}

template <>
hash_t Hash(string_t val) {
	// If the string is inlined, we can do a branchless hash
	if (val.IsInlined()) {
		// This seed slightly improves bit distribution, taken from here:
		// https://github.com/martinus/robin-hood-hashing/blob/3.11.5/LICENSE
		// MIT License Copyright (c) 2018-2021 Martin Ankerl
		hash_t h = 0xe17a1465U ^ (val.GetSize() * 0xc6a4a7935bd1e995U);

		// Hash/combine the first 8-byte block
		const bool not_an_empty_string = !val.Empty();
		h ^= Load<hash_t>(const_data_ptr_cast(val.GetPrefix()));
		h *= 0xd6e8feb86659fd93U * not_an_empty_string + (1 - not_an_empty_string);

		// Load remaining 4 bytes
		hash_t hr = 0;
		memcpy(&hr, const_data_ptr_cast(val.GetPrefix()) + sizeof(hash_t), 4U);

		// Process the remainder the same an 8-byte block
		// This operation is a NOP if the string is <= 8 bytes
		const bool not_a_nop = val.GetSize() > sizeof(hash_t);
		h ^= hr;
		h *= 0xd6e8feb86659fd93U * not_a_nop + (1 - not_a_nop);

		// Finalize
		h = Hash(h);

		// This is just an optimization. It should not change the result
		// This property is important for verification (e.g., DUCKDB_DEBUG_NO_INLINE)
		// We achieved this with the NOP trick above (and in HashBytes)
		D_ASSERT(h == Hash(val.GetData(), val.GetSize()));

		return h;
	}
	return Hash(val.GetData(), val.GetSize());
}

template <>
hash_t Hash(char *val) {
	return Hash<const char *>(val);
}

hash_t HashBytes(const_data_ptr_t ptr, const idx_t len) noexcept {
	// This seed slightly improves bit distribution, taken from here:
	// https://github.com/martinus/robin-hood-hashing/blob/3.11.5/LICENSE
	// MIT License Copyright (c) 2018-2021 Martin Ankerl
	hash_t h = 0xe17a1465U ^ (len * 0xc6a4a7935bd1e995U);

	// Hash/combine in blocks of 8 bytes
	for (const auto end = ptr + len - (len & 7U); ptr != end; ptr += 8U) {
		h ^= Load<hash_t>(ptr);
		h *= 0xd6e8feb86659fd93U;
	}

	// Load remaining (<8) bytes
	hash_t hr = 0;
	memcpy(&hr, ptr, len & 7U);

	// Process the remainder same as an 8-byte block
	// This operation is a NOP if the number of remaining bytes is 0
	const bool not_a_nop = len & 7U;
	h ^= hr;
	h *= 0xd6e8feb86659fd93U * not_a_nop + (1 - not_a_nop);

	// Finalize
	return Hash(h);
}

hash_t Hash(const char *val, size_t size) {
	return HashBytes(const_data_ptr_cast(val), size);
}

hash_t Hash(uint8_t *val, size_t size) {
	return HashBytes(const_data_ptr_cast(val), size);
}

} // namespace duckdb
