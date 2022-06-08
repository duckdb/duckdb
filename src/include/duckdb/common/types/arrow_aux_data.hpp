//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/arrow_aux_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/vector_buffer.hpp"
#include "duckdb/common/arrow_wrapper.hpp"

namespace duckdb {

struct ArrowAuxiliaryData : VectorAuxiliaryData {
	explicit ArrowAuxiliaryData(shared_ptr<ArrowArrayWrapper> arrow_array_p)
	    : VectorAuxiliaryData(VectorAuxiliaryDataType::ARROW_AUXILIARY), arrow_array(std::move(arrow_array_p)) {
	}
	~ArrowAuxiliaryData() override {
	}

	shared_ptr<ArrowArrayWrapper> arrow_array;
};

} // namespace duckdb