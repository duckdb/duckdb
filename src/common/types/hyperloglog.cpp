#include "duckdb/common/types/hyperloglog.hpp"

#include "duckdb/common/exception.hpp"
#include "hyperloglog.hpp"

namespace duckdb {

HyperLogLog::HyperLogLog() : hll(nullptr) {
	hll = duckdb_hll::hll_create();
}

HyperLogLog::HyperLogLog(void *hll) : hll(hll) {
}

HyperLogLog::~HyperLogLog() {
	duckdb_hll::hll_destroy((duckdb_hll::robj *)hll);
}

void HyperLogLog::Add(data_ptr_t element, idx_t size) {
	if (duckdb_hll::hll_add((duckdb_hll::robj *)hll, element, size) == HLL_C_ERR) {
		throw InternalException("Could not add to HLL?");
	}
}

idx_t HyperLogLog::Count() {
	// exception from size_t ban
	size_t result;

	if (duckdb_hll::hll_count((duckdb_hll::robj *)hll, &result) != HLL_C_OK) {
		throw InternalException("Could not count HLL?");
	}
	return result;
}

unique_ptr<HyperLogLog> HyperLogLog::Merge(HyperLogLog &other) {
	duckdb_hll::robj *hlls[2];
	hlls[0] = (duckdb_hll::robj *)hll;
	hlls[1] = (duckdb_hll::robj *)other.hll;
	auto new_hll = duckdb_hll::hll_merge(hlls, 2);
	if (!new_hll) {
		throw InternalException("Could not merge HLLs");
	}
	return unique_ptr<HyperLogLog>(new HyperLogLog((void *)new_hll));
}

HyperLogLog *HyperLogLog::MergePointer(HyperLogLog &other) {
	duckdb_hll::robj *hlls[2];
	hlls[0] = (duckdb_hll::robj *)hll;
	hlls[1] = (duckdb_hll::robj *)other.hll;
	auto new_hll = duckdb_hll::hll_merge(hlls, 2);
	if (!new_hll) {
		throw Exception("Could not merge HLLs");
	}
	return new HyperLogLog((void *)new_hll);
}

unique_ptr<HyperLogLog> HyperLogLog::Merge(HyperLogLog logs[], idx_t count) {
	auto hlls_uptr = unique_ptr<duckdb_hll::robj *[]> {
		new duckdb_hll::robj *[count]
	};
	auto hlls = hlls_uptr.get();
	for (idx_t i = 0; i < count; i++) {
		hlls[i] = (duckdb_hll::robj *)logs[i].hll;
	}
	auto new_hll = duckdb_hll::hll_merge(hlls, count);
	if (!new_hll) {
		throw InternalException("Could not merge HLLs");
	}
	return unique_ptr<HyperLogLog>(new HyperLogLog((void *)new_hll));
}

//===--------------------------------------------------------------------===//
// Vectorized HLL implementation
//===--------------------------------------------------------------------===//
static inline void MurMurHash64ALoopBody(uint64_t &h, const uint8_t *&data) {
	uint64_t k;

#if (BYTE_ORDER == LITTLE_ENDIAN)
	memcpy(&k, data, sizeof(uint64_t));
#else
	k = (uint64_t)data[0];
	k |= (uint64_t)data[1] << 8;
	k |= (uint64_t)data[2] << 16;
	k |= (uint64_t)data[3] << 24;
	k |= (uint64_t)data[4] << 32;
	k |= (uint64_t)data[5] << 40;
	k |= (uint64_t)data[6] << 48;
	k |= (uint64_t)data[7] << 56;
#endif

	k *= HyperLogLog::M;
	k ^= k >> HyperLogLog::R;
	k *= HyperLogLog::M;
	h ^= k;
	h *= HyperLogLog::M;
	data += 8;
}

static inline void MurMurHash64AFinalize(uint64_t &h) {
	h ^= h >> HyperLogLog::R;
	h *= HyperLogLog::M;
	h ^= h >> HyperLogLog::R;
}

template <class T>
inline uint64_t TemplatedMurmurHash64A(const T &elem) {
	uint64_t h = HyperLogLog::SEED ^ (sizeof(T) * HyperLogLog::M);
	const uint8_t *data = (const uint8_t *)&elem;

	for (idx_t i = 0; i < sizeof(T) / 8; i += 8) {
		MurMurHash64ALoopBody(h, data);
	}

	switch (sizeof(T) & 7) {
	case 7:
		h ^= (uint64_t)data[6] << 48; /* fall-thru */
	case 6:
		h ^= (uint64_t)data[5] << 40; /* fall-thru */
	case 5:
		h ^= (uint64_t)data[4] << 32; /* fall-thru */
	case 4:
		h ^= (uint64_t)data[3] << 24; /* fall-thru */
	case 3:
		h ^= (uint64_t)data[2] << 16; /* fall-thru */
	case 2:
		h ^= (uint64_t)data[1] << 8; /* fall-thru */
	case 1:
		h ^= (uint64_t)data[0];
		h *= HyperLogLog::M; /* fall-thru */
	case 0:
		MurMurHash64AFinalize(h);
	};

	return h;
}

template <>
inline uint64_t TemplatedMurmurHash64A(const string_t &elem) {
	const auto &len = elem.GetSize();
	uint64_t h = HyperLogLog::SEED ^ (len * HyperLogLog::M);
	const uint8_t *data = (uint8_t *)elem.GetDataUnsafe();

	for (idx_t i = 0; i < len / 8; i += 8) {
		MurMurHash64ALoopBody(h, data);
	}

	switch (len & 7) {
	case 7:
		h ^= (uint64_t)data[6] << 48; /* fall-thru */
	case 6:
		h ^= (uint64_t)data[5] << 40; /* fall-thru */
	case 5:
		h ^= (uint64_t)data[4] << 32; /* fall-thru */
	case 4:
		h ^= (uint64_t)data[3] << 24; /* fall-thru */
	case 3:
		h ^= (uint64_t)data[2] << 16; /* fall-thru */
	case 2:
		h ^= (uint64_t)data[1] << 8; /* fall-thru */
	case 1:
		h ^= (uint64_t)data[0];
		h *= HyperLogLog::M; /* fall-thru */
	case 0:
		MurMurHash64AFinalize(h);
	};

	return h;
}

template <class T>
void TemplatedComputeHashes(VectorData &vdata, const idx_t &count, uint64_t hashes[]) {
	T *data = (T *)vdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (vdata.validity.RowIsValid(idx)) {
			hashes[i] = TemplatedMurmurHash64A<T>(data[idx]);
		}
	}
}

void HyperLogLog::ComputeHashes(VectorData &vdata, PhysicalType type, idx_t count, uint64_t hashes[]) {
	switch (type) {
	case PhysicalType::UINT8:
		return TemplatedComputeHashes<uint8_t>(vdata, count, hashes);
	case PhysicalType::UINT16:
		return TemplatedComputeHashes<uint16_t>(vdata, count, hashes);
	case PhysicalType::UINT32:
		return TemplatedComputeHashes<uint32_t>(vdata, count, hashes);
	case PhysicalType::UINT64:
		return TemplatedComputeHashes<uint64_t>(vdata, count, hashes);
	case PhysicalType::INT8:
		return TemplatedComputeHashes<int8_t>(vdata, count, hashes);
	case PhysicalType::INT16:
		return TemplatedComputeHashes<int16_t>(vdata, count, hashes);
	case PhysicalType::INT32:
		return TemplatedComputeHashes<int32_t>(vdata, count, hashes);
	case PhysicalType::INT64:
		return TemplatedComputeHashes<int64_t>(vdata, count, hashes);
	case PhysicalType::INT128:
		return TemplatedComputeHashes<hugeint_t>(vdata, count, hashes);
	case PhysicalType::FLOAT:
		return TemplatedComputeHashes<float>(vdata, count, hashes);
	case PhysicalType::DOUBLE:
		return TemplatedComputeHashes<double>(vdata, count, hashes);
	case PhysicalType::VARCHAR:
		return TemplatedComputeHashes<string_t>(vdata, count, hashes);
	default:
		throw InternalException("Unimplemented type for HyperLogLog::AddVector");
	}
}

int HyperLogLog::AddHash(uint64_t &hash) {
	uint64_t index = hash & ((1 << 14) - 1); /* Register index. */
	hash >>= 14;                             /* Remove bits used to address the register. */
	hash |= ((uint64_t)1 << (64 - 14));      /* Make sure the loop terminates
	                                          and count will be <= Q+1. */
	uint64_t bit = 1;
	int count = 1; /* Initialized to 1 since we count the "00000...1" pattern. */
	while ((hash & bit) == 0) {
		count++;
		bit <<= 1;
	}

	auto o = (duckdb_hll::robj *)hll;
	duckdb_hll::hllhdr *hdr = (duckdb_hll::hllhdr *)o->ptr;
	switch (hdr->encoding) {
	case 0:
		return duckdb_hll::hllDenseSet(hdr->registers + 1, index, count);
	case 1:
		return duckdb_hll::hllSparseSet(o, index, count);
	default:
		return -1;
	}
}

void HyperLogLog::AddHashes(VectorData &vdata, idx_t count, uint64_t hashes[]) {
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (vdata.validity.RowIsValid(idx)) {
			AddHash(hashes[i]);
		}
	}
}

} // namespace duckdb