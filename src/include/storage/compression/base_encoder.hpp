//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/compression/base_encoder.hpp
//
//
//===----------------------------------------------------------------------===//

#include "common/constants.hpp"

namespace duckdb {
class EncodedBlock;
class DataChunk;

/*!
 * Base class for Encoders
 */
class BaseEncoder {
public:
	virtual ~BaseEncoder() = default;
	//! Encodes a set of chucks into and encoded block
	virtual std::unique_ptr<const EncodedBlock> encode(const std::vector<DataChunk> &chunks) = 0;

	virtual std::vector<DataChunk> decode(const EncodedBlock block) = 0;

	virtual std::unique_ptr<BaseEncoder> getEncoder() const = 0;
};

} // namespace duckdb
