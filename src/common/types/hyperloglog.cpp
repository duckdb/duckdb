#include "duckdb/common/types/hyperloglog.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

#include <math.h>

namespace duckdb {

idx_t HyperLogLog::Count() const {
	uint32_t c[Q + 2] = {0};
	ExtractCounts(c);
	return static_cast<idx_t>(EstimateCardinality(c));
}

//! Algorithm 2
void HyperLogLog::Merge(const HyperLogLog &other) {
	for (idx_t i = 0; i < M; ++i) {
		Update(i, other.k[i]);
	}
}

//! Algorithm 4
void HyperLogLog::ExtractCounts(uint32_t *c) const {
	for (idx_t i = 0; i < M; ++i) {
		c[k[i]]++;
	}
}

//! Taken from redis code
static double HLLSigma(double x) {
	if (x == 1.) {
		return std::numeric_limits<double>::infinity();
	}
	double z_prime;
	double y = 1;
	double z = x;
	do {
		x *= x;
		z_prime = z;
		z += x * y;
		y += y;
	} while (z_prime != z);
	return z;
}

//! Taken from redis code
static double HLLTau(double x) {
	if (x == 0. || x == 1.) {
		return 0.;
	}
	double z_prime;
	double y = 1.0;
	double z = 1 - x;
	do {
		x = sqrt(x);
		z_prime = z;
		y *= 0.5;
		z -= pow(1 - x, 2) * y;
	} while (z_prime != z);
	return z / 3;
}

//! Algorithm 6
int64_t HyperLogLog::EstimateCardinality(uint32_t *c) {
	auto z = M * HLLTau((double(M) - c[Q]) / double(M));

	for (idx_t k = Q; k >= 1; --k) {
		z += c[k];
		z *= 0.5;
	}

	z += M * HLLSigma(c[0] / double(M));

	return llroundl(ALPHA * M * M / z);
}

void HyperLogLog::Update(Vector &input, Vector &hash_vec, const idx_t count) {
	UnifiedVectorFormat idata;
	input.ToUnifiedFormat(count, idata);

	UnifiedVectorFormat hdata;
	hash_vec.ToUnifiedFormat(count, hdata);
	const auto hashes = UnifiedVectorFormat::GetData<hash_t>(hdata);

	if (hash_vec.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (idata.validity.RowIsValid(0)) {
			InsertElement(hashes[0]);
		}
	} else {
		for (idx_t i = 0; i < count; ++i) {
			if (idata.validity.RowIsValid(idata.sel->get_index(i))) {
				const auto hash = hashes[hdata.sel->get_index(i)];
				InsertElement(hash);
			}
		}
	}
}

unique_ptr<HyperLogLog> HyperLogLog::Copy() const {
	auto result = make_uniq<HyperLogLog>();
	memcpy(result->k, this->k, sizeof(k));
	D_ASSERT(result->Count() == Count());
	return result;
}

void HyperLogLog::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "type", HLLStorageType::HLL_V2);
	serializer.WriteProperty(101, "data", k, sizeof(k));
}

unique_ptr<HyperLogLog> HyperLogLog::Deserialize(Deserializer &deserializer) {
	static constexpr idx_t HLL_V1_SIZE = 3089;

	auto result = make_uniq<HyperLogLog>();
	auto storage_type = deserializer.ReadProperty<HLLStorageType>(100, "type");
	switch (storage_type) {
	case HLLStorageType::HLL_V1: {
		auto dummy_array = make_uniq_array_uninitialized<data_t>(HLL_V1_SIZE);
		deserializer.ReadProperty(101, "data", dummy_array.get(), HLL_V1_SIZE);
		break; // Deprecated
	}
	case HLLStorageType::HLL_V2:
		deserializer.ReadProperty(101, "data", result->k, sizeof(k));
		break;
	default:
		throw SerializationException("Unknown HyperLogLog storage type!");
	}
	return result;
}

} // namespace duckdb
