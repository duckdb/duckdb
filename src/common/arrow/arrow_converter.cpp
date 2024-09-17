#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/bit.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/sel_cache.hpp"
#include "duckdb/common/types/vector_cache.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include <list>
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_schema_constructor.hpp"

namespace duckdb {

void ArrowConverter::ToArrowArray(DataChunk &input, ArrowArray *out_array, ClientProperties options) {
	ArrowAppender appender(input.GetTypes(), input.size(), std::move(options));
	appender.Append(input, 0, input.size(), input.size());
	*out_array = appender.Finalize();
}

void ArrowConverter::ToArrowSchema(ArrowSchema &out_schema, const vector<LogicalType> &types,
                                   const vector<string> &names, const ClientProperties &options, bool add_metadata) {
	ArrowSchemaConstructor constructor(options, add_metadata);
	constructor.Construct(out_schema, types, names);
	return;
}

} // namespace duckdb
