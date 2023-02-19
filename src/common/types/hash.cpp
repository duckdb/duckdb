#include "duckdb/common/types/hash.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/interval.hpp"

#include <functional>
#include <cmath>

namespace duckdb {

template <>
hash_t Hash(uint64_t val) {
	return murmurhash64(val);
}

template <>
hash_t Hash(int64_t val) {
	return murmurhash64((uint64_t)val);
}

template <>
hash_t Hash(hugeint_t val) {
	return murmurhash64(val.lower) ^ murmurhash64(val.upper);
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
	uint32_t uval = Load<uint32_t>((const_data_ptr_t)&val);
	return murmurhash64(uval);
}

template <>
hash_t Hash(double val) {
	static_assert(sizeof(double) == sizeof(uint64_t), "");
	FloatingPointEqualityTransform<double>::OP(val);
	uint64_t uval = Load<uint64_t>((const_data_ptr_t)&val);
	return murmurhash64(uval);
}

template <>
hash_t Hash(interval_t val) {
	return Hash(val.days) ^ Hash(val.months) ^ Hash(val.micros);
}

template <>
hash_t Hash(const char *str) {
	return Hash(str, strlen(str));
}

template <>
hash_t Hash(char *val) {
	return Hash<const char *>(val);
}

// MIT License
// Copyright (c) 2018-2021 Martin Ankerl
// https://github.com/martinus/robin-hood-hashing/blob/3.11.5/LICENSE
hash_t HashBytes(const void *ptr, size_t len) noexcept {
	static constexpr uint64_t M = UINT64_C(0xc6a4a7935bd1e995);
	static constexpr uint64_t SEED = UINT64_C(0xe17a1465);
	static constexpr unsigned int R = 47;

	auto const *const data64 = static_cast<uint64_t const *>(ptr);
	uint64_t h = SEED ^ (len * M);

	size_t const n_blocks = len / 8;
	for (size_t i = 0; i < n_blocks; ++i) {
		auto k = Load<uint64_t>(reinterpret_cast<const_data_ptr_t>(data64 + i));

		k *= M;
		k ^= k >> R;
		k *= M;

		h ^= k;
		h *= M;
	}

	union {
		uint64_t u64;
		uint8_t u8[8];
	} u;
	uint64_t &k1 = u.u64;
	k1 = 0;
	auto const *const data8 = reinterpret_cast<uint8_t const *>(data64 + n_blocks);
	switch (len & 7U) {
	case 7:
		u.u8[6] = data8[6];
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case 6:
		u.u8[5] = data8[5];
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case 5:
		u.u8[4] = data8[4];
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case 4:
		u.u8[3] = data8[3];
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case 3:
		u.u8[2] = data8[2];
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case 2:
		u.u8[1] = data8[1];
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case 1:
		u.u8[0] = data8[0];

		k1 *= M;
		k1 ^= k1 >> R;
		k1 *= M;

		h ^= k1;
		break;
	case 0:
		// Nothing to do;
		break;
	}

	h *= M;

	h ^= h >> R;
	h *= M;
	h ^= h >> R;
	return static_cast<hash_t>(h);
}

hash_t Hash(const char *val, size_t size) {
	return HashBytes((const void *)val, size);
}

hash_t Hash(uint8_t *val, size_t size) {
	return HashBytes((void *)val, size);
}

template <>
hash_t Hash(string_t val) {
#ifdef DUCKDB_DEBUG_NO_INLINE
	return HashBytes((const void *)val.GetDataUnsafe(), val.GetSize());
#endif
	if (val.IsInlined()) {
		static constexpr uint64_t M = UINT64_C(0xc6a4a7935bd1e995);
		static constexpr uint64_t SEED = UINT64_C(0xe17a1465);
		static constexpr unsigned int R = 47;

		const_data_ptr_t data64 = static_cast<const_data_ptr_t>(val.getInlined());
		uint64_t h = SEED ^ (val.GetSize() * M);

		uint64_t k = Load<uint64_t>(reinterpret_cast<const_data_ptr_t>(data64));

		k *= M;
		k ^= k >> R;
		k *= M;

		h ^= k;
		h *= M;

		k = Load<uint32_t>(reinterpret_cast<const_data_ptr_t>((char const *)data64 + 8u));
		k <<= 32u;
		k *= M;
		k ^= k >> R;
		k *= M;

		h ^= k;
		h *= M;

		h ^= h >> R;
		h *= M;
		h ^= h >> R;
		return static_cast<hash_t>(h);
	} else {
		return HashBytes((const void *)val.GetDataUnsafe(), val.GetSize());
	}
}

} // namespace duckdb
