#include "duckdb/common/enums/column_segment_info_scan_type.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception/parser_exception.hpp"

namespace duckdb {

ColumnSegmentInfoScanType ColumnSegmentInfoScanTypeFromString(const string &input) {
	auto parameter = StringUtil::Lower(input);
	if (parameter == "all") {
		return ColumnSegmentInfoScanType::ALL;
	} else if (parameter == "only_loaded_segments") {
		return ColumnSegmentInfoScanType::ONLY_LOADED_SEGMENTS;
	} else {
		throw ParserException("Unrecognized column segment info scan type \"%s\"", input);
	}
}

} // namespace duckdb
