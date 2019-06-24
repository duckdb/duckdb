//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/table/transient_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/block.hpp"
#include "storage/table/column_segment.hpp"

namespace duckdb {
class TransientSegment : public ColumnSegment {
public:
    //! Initialize an empty in-memory column segment
    TransientSegment(index_t start);

private:
    //! The data of the column segment
    Block block;
};

}
