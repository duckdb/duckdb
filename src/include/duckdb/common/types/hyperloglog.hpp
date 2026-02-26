//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/hyperloglog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/bit_utils.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

enum class HLLStorageType : uint8_t {
	HLL_V1 = 1, //! Redis HLL
	HLL_V2 = 2, //! Our own implementation
};

class Serializer;
class Deserializer;

//! Algorithms from
//! "New cardinality estimation algorithms for HyperLogLog sketches"
//! Otmar Ertl, arXiv:1702.01284
class HyperLogLogBase {
public:
	static double HLLSigma(double x);
	static double HLLTau(double x);
};

template <idx_t P>
class HyperLogLogP : public HyperLogLogBase {
public:
	static constexpr idx_t Q = 64 - P;
	static constexpr idx_t M = 1 << P;
	static constexpr double ALPHA = 0.721347520444481703680; // 1 / (2 log(2))

	static double GetErrorRate() {
		return std::sqrt(PI / 2.0) / sqrt(M);
	}

public:
	HyperLogLogP() {
		memset(k, 0, sizeof(k));
	}

	//! Algorithm 1
	inline void InsertElement(hash_t h) {
		const auto i = h & ((1 << P) - 1);
		h >>= P;
		h |= hash_t(1) << Q;
		const uint8_t z = UnsafeNumericCast<uint8_t>(CountZeros<hash_t>::Trailing(h) + 1);
		Update(i, z);
	}

	inline void Update(const idx_t &i, const uint8_t &z) {
		k[i] = MaxValue<uint8_t>(k[i], z);
	}

	inline uint8_t GetRegister(const idx_t &i) const {
		return k[i];
	}

	idx_t Count() const {
		uint32_t c[Q + 2] = {0};
		ExtractCounts(c);
		return static_cast<idx_t>(EstimateCardinality(c));
	}

	//! Algorithm 2
	void Merge(const HyperLogLogP &other) {
		for (idx_t i = 0; i < M; ++i) {
			Update(i, other.k[i]);
		}
	}

public:
	//! Add data to this HLL
	void Update(Vector &input, Vector &hash_vec, const idx_t count) {
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
			D_ASSERT(hash_vec.GetVectorType() == VectorType::FLAT_VECTOR);
			if (idata.validity.AllValid()) {
				for (idx_t i = 0; i < count; ++i) {
					const auto hash = hashes[i];
					InsertElement(hash);
				}
			} else {
				for (idx_t i = 0; i < count; ++i) {
					if (idata.validity.RowIsValid(idata.sel->get_index(i))) {
						const auto hash = hashes[i];
						InsertElement(hash);
					}
				}
			}
		}
	};

	//! Algorithm 4
	void ExtractCounts(uint32_t *c) const {
		for (idx_t i = 0; i < M; ++i) {
			c[k[i]]++;
		}
	};

	//! Algorithm 6
	static int64_t EstimateCardinality(uint32_t *c) {
		auto z = M * HLLTau((double(M) - c[Q]) / double(M));

		for (idx_t k = Q; k >= 1; --k) {
			z += c[k];
			z *= 0.5;
		}

		z += M * HLLSigma(c[0] / double(M));

		return llroundl(ALPHA * M * M / z);
	};

protected:
	uint8_t k[M];
};

class HyperLogLog : public HyperLogLogP<6> {
public:
	//! Get copy of the HLL
	unique_ptr<HyperLogLog> Copy() const;

	void Serialize(Serializer &serializer) const;
	static unique_ptr<HyperLogLog> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
