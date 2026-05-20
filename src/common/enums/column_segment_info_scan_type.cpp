#include "duckdb/common/enums/column_segment_info_scan_type.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception/parser_exception.hpp"

namespace duckdb {

ColumnSegmentInfoScanType ColumnSegmentInfoScanTypeFromString(const string &input) {
	auto parameter = StringUtil::Lower(input);
	if (parameter == "standard") {
		return ColumnSegmentInfoScanType::STANDARD;
	} else if (parameter == "extended") {
		return ColumnSegmentInfoScanType::EXTENDED;
	} else if (parameter == "extended_only_loaded") {
		return ColumnSegmentInfoScanType::EXTENDED_ONLY_LOADED;
	} else {
		throw ParserException("Unrecognized column segment info scan type \"%s\"", input);
	}
}

} // namespace duckdb
