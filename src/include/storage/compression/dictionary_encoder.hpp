//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/compression/dictionary_encoder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"
#include "storage/compression/base_encoder.hpp"

namespace duckdb {
class DataChunk;
/*!
 * Base class for Encoders
 */
class DictionaryEncoder : BaseEncoder {
public:
	~DictionaryEncoder() = default;
	//! Encodes a set of chucks into and encoded block
	unique_ptr<const EncodedBlock> encode(const vector<DataChunk> &chunks) = 0;

	vector<DataChunk> decode(const EncodedBlock block) = 0;

	unique_ptr<BaseEncoder> getEncoder() const = 0;
};

} // namespace duckdb
