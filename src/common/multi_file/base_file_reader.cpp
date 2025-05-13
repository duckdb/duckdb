#include "duckdb/common/multi_file/base_file_reader.hpp"

namespace duckdb {

shared_ptr<BaseUnionData> BaseFileReader::GetUnionData(idx_t file_idx) {
	throw NotImplementedException("Union by name not supported for reader of type %s", GetReaderType());
}

}
