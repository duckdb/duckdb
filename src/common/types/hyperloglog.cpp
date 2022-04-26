#include "duckdb/common/types/hyperloglog.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"
#include "hyperloglog.hpp"

namespace duckdb {

HyperLogLog::HyperLogLog() : hll(nullptr) {
	hll = duckdb_hll::hll_create();
	// Insert into a dense hll can be vectorized, sparse cannot, so we immediately convert
	duckdb_hll::hllSparseToDense((duckdb_hll::robj *)hll);
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

idx_t HyperLogLog::Count() const {
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

idx_t HyperLogLog::GetSize() {
	return duckdb_hll::get_size();
}

data_ptr_t HyperLogLog::GetPtr() const {
	return (data_ptr_t)((duckdb_hll::robj *)hll)->ptr;
}

unique_ptr<HyperLogLog> HyperLogLog::Copy() const {
	auto result = make_unique<HyperLogLog>();
	memcpy(result->GetPtr(), GetPtr(), GetSize());
	D_ASSERT(result->Count() == Count());
	return result;
}

void HyperLogLog::Serialize(FieldWriter &writer) const {
	writer.WriteBlob(GetPtr(), GetSize());
}

unique_ptr<HyperLogLog> HyperLogLog::Deserialize(FieldReader &reader) {
	auto result = make_unique<HyperLogLog>();
	reader.ReadBlob(result->GetPtr(), GetSize());
	return result;
}

//===--------------------------------------------------------------------===//
// Vectorized HLL implementation
//===--------------------------------------------------------------------===//
//! Taken from https://nullprogram.com/blog/2018/07/31/
template <class T>
inline uint64_t TemplatedHash(const T &elem) {
	uint64_t x = elem;
	x ^= x >> 30;
	x *= 0xbf58476d1ce4e5b9U;
	x ^= x >> 27;
	x *= 0x94d049bb133111ebU;
	x ^= x >> 31;
	return x;
}

template <>
inline uint64_t TemplatedHash(const hugeint_t &elem) {
	return TemplatedHash<uint64_t>(Load<uint64_t>((data_ptr_t)&elem.upper)) ^ TemplatedHash<uint64_t>(elem.lower);
}

template <>
inline uint64_t TemplatedHash(const interval_t &elem) {
	return TemplatedHash<uint32_t>(Load<uint32_t>((data_ptr_t)&elem.months)) ^
	       TemplatedHash<uint32_t>(Load<uint32_t>((data_ptr_t)&elem.days)) ^
	       TemplatedHash<uint64_t>(Load<uint64_t>((data_ptr_t)&elem.micros));
}

template <idx_t rest>
inline void CreateIntegerRecursive(const data_ptr_t &data, uint64_t &x) {
	x ^= (uint64_t)data[rest - 1] << ((rest - 1) * 8);
	return CreateIntegerRecursive<rest - 1>(data, x);
}

template <>
inline void CreateIntegerRecursive<1>(const data_ptr_t &data, uint64_t &x) {
	x ^= (uint64_t)data[0];
}

inline uint64_t HashOtherSize(const data_ptr_t &data, const idx_t &len) {
	uint64_t x = 0;
	switch (len & 7) {
	case 7:
		CreateIntegerRecursive<7>(data, x);
		break;
	case 6:
		CreateIntegerRecursive<6>(data, x);
		break;
	case 5:
		CreateIntegerRecursive<5>(data, x);
		break;
	case 4:
		CreateIntegerRecursive<4>(data, x);
		break;
	case 3:
		CreateIntegerRecursive<3>(data, x);
		break;
	case 2:
		CreateIntegerRecursive<2>(data, x);
		break;
	case 1:
		CreateIntegerRecursive<1>(data, x);
		break;
	case 0:
		throw InternalException("Rest 0 in HashOtherSize!");
	}
	return TemplatedHash<uint64_t>(x);
}

template <>
inline uint64_t TemplatedHash(const string_t &elem) {
	data_ptr_t data = (data_ptr_t)elem.GetDataUnsafe();
	const auto &len = elem.GetSize();
	uint64_t h = 0;
	for (idx_t i = 0; i < len / 8; i += 8) {
		h ^= TemplatedHash<uint64_t>(Load<uint64_t>(data));
		data += 8;
	}
	switch (len & 7) {
	case 4:
		h ^= TemplatedHash<uint32_t>(Load<uint32_t>(data));
		break;
	case 2:
		h ^= TemplatedHash<uint16_t>(Load<uint16_t>(data));
		break;
	case 1:
		h ^= TemplatedHash<uint8_t>(Load<uint8_t>(data));
		break;
	default:
		h ^= HashOtherSize(data, len);
	}
	return h;
}

template <class T>
void TemplatedComputeHashes(VectorData &vdata, const idx_t &count, uint64_t hashes[]) {
	T *data = (T *)vdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (vdata.validity.RowIsValid(idx)) {
			hashes[i] = TemplatedHash<T>(data[idx]);
		}
	}
}

static void ComputeHashes(VectorData &vdata, PhysicalType type, uint64_t hashes[], idx_t count) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::UINT8:
		return TemplatedComputeHashes<uint8_t>(vdata, count, hashes);
	case PhysicalType::INT16:
	case PhysicalType::UINT16:
		return TemplatedComputeHashes<uint16_t>(vdata, count, hashes);
	case PhysicalType::INT32:
	case PhysicalType::UINT32:
	case PhysicalType::FLOAT:
		return TemplatedComputeHashes<uint32_t>(vdata, count, hashes);
	case PhysicalType::INT64:
	case PhysicalType::UINT64:
	case PhysicalType::DOUBLE:
		return TemplatedComputeHashes<uint64_t>(vdata, count, hashes);
	case PhysicalType::INT128:
		return TemplatedComputeHashes<hugeint_t>(vdata, count, hashes);
	case PhysicalType::INTERVAL:
		return TemplatedComputeHashes<interval_t>(vdata, count, hashes);
	case PhysicalType::VARCHAR:
		return TemplatedComputeHashes<string_t>(vdata, count, hashes);
	default:
		throw InternalException("Unimplemented type for HyperLogLog::ComputeHashes");
	}
}

static const uint64_t bits[] = {
    (uint64_t)1 << 0,  (uint64_t)1 << 1,  (uint64_t)1 << 2,  (uint64_t)1 << 3,  (uint64_t)1 << 4,  (uint64_t)1 << 5,
    (uint64_t)1 << 6,  (uint64_t)1 << 7,  (uint64_t)1 << 8,  (uint64_t)1 << 9,  (uint64_t)1 << 10, (uint64_t)1 << 11,
    (uint64_t)1 << 12, (uint64_t)1 << 13, (uint64_t)1 << 14, (uint64_t)1 << 15, (uint64_t)1 << 16, (uint64_t)1 << 17,
    (uint64_t)1 << 18, (uint64_t)1 << 19, (uint64_t)1 << 20, (uint64_t)1 << 21, (uint64_t)1 << 22, (uint64_t)1 << 23,
    (uint64_t)1 << 24, (uint64_t)1 << 25, (uint64_t)1 << 26, (uint64_t)1 << 27, (uint64_t)1 << 28, (uint64_t)1 << 29,
    (uint64_t)1 << 30, (uint64_t)1 << 31, (uint64_t)1 << 32, (uint64_t)1 << 33, (uint64_t)1 << 34, (uint64_t)1 << 35,
    (uint64_t)1 << 36, (uint64_t)1 << 37, (uint64_t)1 << 38, (uint64_t)1 << 39, (uint64_t)1 << 40, (uint64_t)1 << 41,
    (uint64_t)1 << 42, (uint64_t)1 << 43, (uint64_t)1 << 44, (uint64_t)1 << 45, (uint64_t)1 << 46, (uint64_t)1 << 47,
    (uint64_t)1 << 48, (uint64_t)1 << 49, (uint64_t)1 << 50, (uint64_t)1 << 51, (uint64_t)1 << 52, (uint64_t)1 << 53,
    (uint64_t)1 << 54, (uint64_t)1 << 55, (uint64_t)1 << 56, (uint64_t)1 << 57, (uint64_t)1 << 58, (uint64_t)1 << 59,
    (uint64_t)1 << 60, (uint64_t)1 << 61, (uint64_t)1 << 62, (uint64_t)1 << 63};

static inline void ComputeIndexAndCount(uint64_t &hash, uint8_t &prefix) {
	uint64_t index = hash & ((1 << 14) - 1); /* Register index. */
	hash >>= 14;                             /* Remove bits used to address the register. */
	hash |= ((uint64_t)1 << (64 - 14));      /* Make sure the loop terminates
	                                          and count will be <= Q+1. */
	idx_t i = 0;
	while (hash & bits[i]) {
		i++;
	}

	hash = index;
	prefix = i + 1; /* Add 1 since we count the "00000...1" pattern. */
}

void HyperLogLog::ProcessEntries(VectorData &vdata, PhysicalType type, uint64_t hashes[], uint8_t counts[],
                                 idx_t count) {
	ComputeHashes(vdata, type, hashes, count);
	for (idx_t i = 0; i < count; i++) {
		ComputeIndexAndCount(hashes[i], counts[i]);
	}
}

void HyperLogLog::AddToLogs(VectorData &vdata, idx_t count, uint64_t indices[], uint8_t counts[], HyperLogLog **logs[],
                            const SelectionVector *log_sel) {
	AddToLogsInternal(vdata, count, indices, counts, (void ****)logs, log_sel);
}

void HyperLogLog::AddToLog(VectorData &vdata, idx_t count, uint64_t indices[], uint8_t counts[]) {
	AddToSingleLogInternal(vdata, count, indices, counts, hll);
}

} // namespace duckdb
