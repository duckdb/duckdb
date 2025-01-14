#include "duckdb/common/box_renderer.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "utf8proc_wrapper.hpp"

#include <sstream>

namespace duckdb {

const idx_t BoxRenderer::SPLIT_COLUMN = idx_t(-1);

//===--------------------------------------------------------------------===//
// Result Renderer
//===--------------------------------------------------------------------===//
BaseResultRenderer::BaseResultRenderer() : value_type(LogicalTypeId::INVALID) {
}

BaseResultRenderer::~BaseResultRenderer() {
}

BaseResultRenderer &BaseResultRenderer::operator<<(char c) {
	RenderLayout(string(1, c));
	return *this;
}

BaseResultRenderer &BaseResultRenderer::operator<<(const string &val) {
	RenderLayout(val);
	return *this;
}

void BaseResultRenderer::Render(ResultRenderType render_mode, const string &val) {
	switch (render_mode) {
	case ResultRenderType::LAYOUT:
		RenderLayout(val);
		break;
	case ResultRenderType::COLUMN_NAME:
		RenderColumnName(val);
		break;
	case ResultRenderType::COLUMN_TYPE:
		RenderType(val);
		break;
	case ResultRenderType::VALUE:
		RenderValue(val, value_type);
		break;
	case ResultRenderType::NULL_VALUE:
		RenderNull(val, value_type);
		break;
	case ResultRenderType::FOOTER:
		RenderFooter(val);
		break;
	default:
		throw InternalException("Unsupported type for result renderer");
	}
}

void BaseResultRenderer::SetValueType(const LogicalType &type) {
	value_type = type;
}

void StringResultRenderer::RenderLayout(const string &text) {
	result += text;
}

void StringResultRenderer::RenderColumnName(const string &text) {
	result += text;
}

void StringResultRenderer::RenderType(const string &text) {
	result += text;
}

void StringResultRenderer::RenderValue(const string &text, const LogicalType &type) {
	result += text;
}

void StringResultRenderer::RenderNull(const string &text, const LogicalType &type) {
	result += text;
}

void StringResultRenderer::RenderFooter(const string &text) {
	result += text;
}

const string &StringResultRenderer::str() {
	return result;
}

//===--------------------------------------------------------------------===//
// Box Renderer
//===--------------------------------------------------------------------===//
BoxRenderer::BoxRenderer(BoxRendererConfig config_p) : config(std::move(config_p)) {
}

string BoxRenderer::ToString(ClientContext &context, const vector<string> &names, const ColumnDataCollection &result) {
	StringResultRenderer ss;
	Render(context, names, result, ss);
	return ss.str();
}

void BoxRenderer::Print(ClientContext &context, const vector<string> &names, const ColumnDataCollection &result) {
	Printer::Print(ToString(context, names, result));
}

void BoxRenderer::RenderValue(BaseResultRenderer &ss, const string &value, idx_t column_width,
                              ResultRenderType render_mode, ValueRenderAlignment alignment) {
	auto render_width = Utf8Proc::RenderWidth(value);

	const string *render_value = &value;
	string small_value;
	if (render_width > column_width) {
		// the string is too large to fit in this column!
		// the size of this column must have been reduced
		// figure out how much of this value we can render
		idx_t pos = 0;
		idx_t current_render_width = config.DOTDOTDOT_LENGTH;
		while (pos < value.size()) {
			// check if this character fits...
			auto char_size = Utf8Proc::RenderWidth(value.c_str(), value.size(), pos);
			if (current_render_width + char_size >= column_width) {
				// it doesn't! stop
				break;
			}
			// it does! move to the next character
			current_render_width += char_size;
			pos = Utf8Proc::NextGraphemeCluster(value.c_str(), value.size(), pos);
		}
		small_value = value.substr(0, pos) + config.DOTDOTDOT;
		render_value = &small_value;
		render_width = current_render_width;
	}
	auto padding_count = (column_width - render_width) + 2;
	idx_t lpadding;
	idx_t rpadding;
	switch (alignment) {
	case ValueRenderAlignment::LEFT:
		lpadding = 1;
		rpadding = padding_count - 1;
		break;
	case ValueRenderAlignment::MIDDLE:
		lpadding = padding_count / 2;
		rpadding = padding_count - lpadding;
		break;
	case ValueRenderAlignment::RIGHT:
		lpadding = padding_count - 1;
		rpadding = 1;
		break;
	default:
		throw InternalException("Unrecognized value renderer alignment");
	}
	ss << config.VERTICAL;
	ss << string(lpadding, ' ');
	ss.Render(render_mode, *render_value);
	ss << string(rpadding, ' ');
}

string BoxRenderer::RenderType(const LogicalType &type) {
	if (type.HasAlias()) {
		return StringUtil::Lower(type.ToString());
	}
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return "int8";
	case LogicalTypeId::SMALLINT:
		return "int16";
	case LogicalTypeId::INTEGER:
		return "int32";
	case LogicalTypeId::BIGINT:
		return "int64";
	case LogicalTypeId::HUGEINT:
		return "int128";
	case LogicalTypeId::UTINYINT:
		return "uint8";
	case LogicalTypeId::USMALLINT:
		return "uint16";
	case LogicalTypeId::UINTEGER:
		return "uint32";
	case LogicalTypeId::UBIGINT:
		return "uint64";
	case LogicalTypeId::UHUGEINT:
		return "uint128";
	case LogicalTypeId::LIST: {
		auto child = RenderType(ListType::GetChildType(type));
		return child + "[]";
	}
	default:
		return StringUtil::Lower(type.ToString());
	}
}

ValueRenderAlignment BoxRenderer::TypeAlignment(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return ValueRenderAlignment::RIGHT;
	default:
		return ValueRenderAlignment::LEFT;
	}
}

string BoxRenderer::TryFormatLargeNumber(const string &numeric) {
	// we only return a readable rendering if the number is > 1 million
	if (numeric.size() <= 5) {
		// number too small for sure
		return string();
	}
	// get the number to summarize
	idx_t number = 0;
	bool negative = false;
	idx_t i = 0;
	if (numeric[0] == '-') {
		negative = true;
		i++;
	}
	for (; i < numeric.size(); i++) {
		char c = numeric[i];
		if (c == '.') {
			break;
		}
		if (c < '0' || c > '9') {
			// not a number or something funky (e.g. 1.23e7)
			// we could theoretically summarize numbers with exponents
			return string();
		}
		if (number >= 1000000000000000000ULL) {
			// number too big
			return string();
		}
		number = number * 10 + static_cast<idx_t>(c - '0');
	}
	struct UnitBase {
		idx_t base;
		const char *name;
	};
	static constexpr idx_t BASE_COUNT = 5;
	UnitBase bases[] = {{1000000ULL, "million"},
	                    {1000000000ULL, "billion"},
	                    {1000000000000ULL, "trillion"},
	                    {1000000000000000ULL, "quadrillion"},
	                    {1000000000000000000ULL, "quintillion"}};
	idx_t base = 0;
	string unit;
	for (idx_t i = 0; i < BASE_COUNT; i++) {
		// round the number according to this base
		idx_t rounded_number = number + ((bases[i].base / 100ULL) / 2);
		if (rounded_number >= bases[i].base) {
			base = bases[i].base;
			unit = bases[i].name;
		}
	}
	if (unit.empty()) {
		return string();
	}
	number += (base / 100ULL) / 2;
	idx_t decimal_unit = number / (base / 100ULL);
	string decimal_str = to_string(decimal_unit);
	string result;
	if (negative) {
		result += "-";
	}
	result += decimal_str.substr(0, decimal_str.size() - 2);
	result += config.decimal_separator == '\0' ? '.' : config.decimal_separator;
	result += decimal_str.substr(decimal_str.size() - 2, 2);
	result += " ";
	result += unit;
	return result;
}

list<ColumnDataCollection> BoxRenderer::FetchRenderCollections(ClientContext &context,
                                                               const ColumnDataCollection &result, idx_t top_rows,
                                                               idx_t bottom_rows) {
	auto column_count = result.ColumnCount();
	vector<LogicalType> varchar_types;
	for (idx_t c = 0; c < column_count; c++) {
		varchar_types.emplace_back(LogicalType::VARCHAR);
	}
	std::list<ColumnDataCollection> collections;
	collections.emplace_back(context, varchar_types);
	collections.emplace_back(context, varchar_types);

	auto &top_collection = collections.front();
	auto &bottom_collection = collections.back();

	DataChunk fetch_result;
	fetch_result.Initialize(context, result.Types());

	DataChunk insert_result;
	insert_result.Initialize(context, varchar_types);

	if (config.large_number_rendering == LargeNumberRendering::FOOTER) {
		if (config.render_mode != RenderMode::ROWS || result.Count() != 1) {
			// large number footer can only be constructed (1) if we have a single row, and (2) in ROWS mode
			config.large_number_rendering = LargeNumberRendering::NONE;
		}
	}

	// fetch the top rows from the ColumnDataCollection
	idx_t chunk_idx = 0;
	idx_t row_idx = 0;
	while (row_idx < top_rows) {
		fetch_result.Reset();
		insert_result.Reset();
		// fetch the next chunk
		result.FetchChunk(chunk_idx, fetch_result);
		idx_t insert_count = MinValue<idx_t>(fetch_result.size(), top_rows - row_idx);

		// cast all columns to varchar
		for (idx_t c = 0; c < column_count; c++) {
			VectorOperations::Cast(context, fetch_result.data[c], insert_result.data[c], insert_count);
		}
		insert_result.SetCardinality(insert_count);

		// construct the render collection
		top_collection.Append(insert_result);

		// if we have are constructing a footer
		if (config.large_number_rendering == LargeNumberRendering::FOOTER) {
			D_ASSERT(insert_count == 1);
			vector<string> readable_numbers;
			readable_numbers.resize(column_count);
			bool all_readable = true;
			for (idx_t c = 0; c < column_count; c++) {
				if (!result.Types()[c].IsNumeric()) {
					// not a numeric type - cannot summarize
					all_readable = false;
					break;
				}
				// add a readable rendering of the value (i.e. "1234567" becomes "1.23 million")
				// we only add the rendering if the string is big
				auto numeric_val = insert_result.data[c].GetValue(0).ToString();
				readable_numbers[c] = TryFormatLargeNumber(numeric_val);
				if (readable_numbers[c].empty()) {
					all_readable = false;
					break;
				}
				readable_numbers[c] = "(" + readable_numbers[c] + ")";
			}
			insert_result.Reset();
			if (all_readable) {
				for (idx_t c = 0; c < column_count; c++) {
					insert_result.data[c].SetValue(0, Value(readable_numbers[c]));
				}
				insert_result.SetCardinality(1);
				top_collection.Append(insert_result);
			}
		}

		chunk_idx++;
		row_idx += fetch_result.size();
	}

	// fetch the bottom rows from the ColumnDataCollection
	row_idx = 0;
	chunk_idx = result.ChunkCount() - 1;
	while (row_idx < bottom_rows) {
		fetch_result.Reset();
		insert_result.Reset();
		// fetch the next chunk
		result.FetchChunk(chunk_idx, fetch_result);
		idx_t insert_count = MinValue<idx_t>(fetch_result.size(), bottom_rows - row_idx);

		// invert the rows
		SelectionVector inverted_sel(insert_count);
		for (idx_t r = 0; r < insert_count; r++) {
			inverted_sel.set_index(r, fetch_result.size() - r - 1);
		}

		for (idx_t c = 0; c < column_count; c++) {
			Vector slice(fetch_result.data[c], inverted_sel, insert_count);
			VectorOperations::Cast(context, slice, insert_result.data[c], insert_count);
		}
		insert_result.SetCardinality(insert_count);
		// construct the render collection
		bottom_collection.Append(insert_result);

		chunk_idx--;
		row_idx += fetch_result.size();
	}
	return collections;
}

list<ColumnDataCollection> BoxRenderer::PivotCollections(ClientContext &context, list<ColumnDataCollection> input,
                                                         vector<string> &column_names,
                                                         vector<LogicalType> &result_types, idx_t row_count) {
	auto &top = input.front();
	auto &bottom = input.back();

	vector<LogicalType> varchar_types;
	vector<string> new_names;
	new_names.emplace_back("Column");
	new_names.emplace_back("Type");
	varchar_types.emplace_back(LogicalType::VARCHAR);
	varchar_types.emplace_back(LogicalType::VARCHAR);
	for (idx_t r = 0; r < top.Count(); r++) {
		new_names.emplace_back("Row " + to_string(r + 1));
		varchar_types.emplace_back(LogicalType::VARCHAR);
	}
	for (idx_t r = 0; r < bottom.Count(); r++) {
		auto row_index = row_count - bottom.Count() + r + 1;
		new_names.emplace_back("Row " + to_string(row_index));
		varchar_types.emplace_back(LogicalType::VARCHAR);
	}
	//
	DataChunk row_chunk;
	row_chunk.Initialize(Allocator::DefaultAllocator(), varchar_types);
	std::list<ColumnDataCollection> result;
	result.emplace_back(context, varchar_types);
	result.emplace_back(context, varchar_types);
	auto &res_coll = result.front();
	ColumnDataAppendState append_state;
	res_coll.InitializeAppend(append_state);
	for (idx_t c = 0; c < top.ColumnCount(); c++) {
		vector<column_t> column_ids {c};
		auto row_index = row_chunk.size();
		idx_t current_index = 0;
		row_chunk.SetValue(current_index++, row_index, column_names[c]);
		row_chunk.SetValue(current_index++, row_index, RenderType(result_types[c]));
		for (auto &collection : input) {
			for (auto &chunk : collection.Chunks(column_ids)) {
				for (idx_t r = 0; r < chunk.size(); r++) {
					row_chunk.SetValue(current_index++, row_index, chunk.GetValue(0, r));
				}
			}
		}
		row_chunk.SetCardinality(row_chunk.size() + 1);
		if (row_chunk.size() == STANDARD_VECTOR_SIZE || c + 1 == top.ColumnCount()) {
			res_coll.Append(append_state, row_chunk);
			row_chunk.Reset();
		}
	}
	column_names = std::move(new_names);
	result_types = std::move(varchar_types);
	return result;
}

string BoxRenderer::ConvertRenderValue(const string &input) {
	string result;
	result.reserve(input.size());
	for (idx_t c = 0; c < input.size(); c++) {
		data_t byte_value = const_data_ptr_cast(input.c_str())[c];
		if (byte_value < 32) {
			// ASCII control character
			result += "\\";
			switch (input[c]) {
			case 7:
				// bell
				result += 'a';
				break;
			case 8:
				// backspace
				result += 'b';
				break;
			case 9:
				// tab
				result += 't';
				break;
			case 10:
				// newline
				result += 'n';
				break;
			case 11:
				// vertical tab
				result += 'v';
				break;
			case 12:
				// form feed
				result += 'f';
				break;
			case 13:
				// cariage return
				result += 'r';
				break;
			case 27:
				// escape
				result += 'e';
				break;
			default:
				result += to_string(byte_value);
				break;
			}
		} else {
			result += input[c];
		}
	}
	return result;
}

string BoxRenderer::FormatNumber(const string &input) {
	if (config.large_number_rendering == LargeNumberRendering::ALL) {
		// when large number rendering is set to ALL, we try to format all numbers as large numbers
		auto number = TryFormatLargeNumber(input);
		if (!number.empty()) {
			return number;
		}
	}
	if (config.decimal_separator == '\0' && config.thousand_separator == '\0') {
		// no thousand separator
		return input;
	}
	// first check how many digits there are (preceding any decimal point)
	idx_t character_count = 0;
	for (auto c : input) {
		if (!StringUtil::CharacterIsDigit(c)) {
			break;
		}
		character_count++;
	}
	// find the position of the first thousand separator
	idx_t separator_position = character_count % 3 == 0 ? 3 : character_count % 3;
	// now add the thousand separators
	string result;
	for (idx_t c = 0; c < character_count; c++) {
		if (c == separator_position && config.thousand_separator != '\0') {
			result += config.thousand_separator;
			separator_position += 3;
		}
		result += input[c];
	}
	// add any remaining characters
	for (idx_t c = character_count; c < input.size(); c++) {
		if (input[c] == '.' && config.decimal_separator != '\0') {
			result += config.decimal_separator;
		} else {
			result += input[c];
		}
	}
	return result;
}

string BoxRenderer::ConvertRenderValue(const string &input, const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return FormatNumber(input);
	default:
		return ConvertRenderValue(input);
	}
}

string BoxRenderer::GetRenderValue(BaseResultRenderer &ss, ColumnDataRowCollection &rows, idx_t c, idx_t r,
                                   const LogicalType &type, ResultRenderType &render_mode) {
	try {
		render_mode = ResultRenderType::VALUE;
		ss.SetValueType(type);
		auto row = rows.GetValue(c, r);
		if (row.IsNull()) {
			render_mode = ResultRenderType::NULL_VALUE;
			return config.null_value;
		}
		return ConvertRenderValue(StringValue::Get(row), type);
	} catch (std::exception &ex) {
		return "????INVALID VALUE - " + string(ex.what()) + "?????";
	}
}

vector<idx_t> BoxRenderer::ComputeRenderWidths(const vector<string> &names, const vector<LogicalType> &result_types,
                                               list<ColumnDataCollection> &collections, idx_t min_width,
                                               idx_t max_width, vector<idx_t> &column_map, idx_t &total_length) {
	auto column_count = result_types.size();

	vector<idx_t> widths;
	widths.reserve(column_count);
	for (idx_t c = 0; c < column_count; c++) {
		auto name_width = Utf8Proc::RenderWidth(ConvertRenderValue(names[c]));
		auto type_width = Utf8Proc::RenderWidth(RenderType(result_types[c]));
		widths.push_back(MaxValue<idx_t>(name_width, type_width));
	}

	// now iterate over the data in the render collection and find out the true max width
	for (auto &collection : collections) {
		for (auto &chunk : collection.Chunks()) {
			for (idx_t c = 0; c < column_count; c++) {
				auto string_data = FlatVector::GetData<string_t>(chunk.data[c]);
				for (idx_t r = 0; r < chunk.size(); r++) {
					string render_value;
					if (FlatVector::IsNull(chunk.data[c], r)) {
						render_value = config.null_value;
					} else {
						render_value = ConvertRenderValue(string_data[r].GetString(), result_types[c]);
					}
					auto render_width = Utf8Proc::RenderWidth(render_value);
					widths[c] = MaxValue<idx_t>(render_width, widths[c]);
				}
			}
		}
	}

	// figure out the total length
	// we start off with a pipe (|)
	total_length = 1;
	for (idx_t c = 0; c < widths.size(); c++) {
		// each column has a space at the beginning, and a space plus a pipe (|) at the end
		// hence + 3
		total_length += widths[c] + 3;
	}
	if (total_length < min_width) {
		// if there are hidden rows we should always display that
		// stretch up the first column until we have space to show the row count
		widths[0] += min_width - total_length;
		total_length = min_width;
	}
	// now we need to constrain the length
	unordered_set<idx_t> pruned_columns;
	if (total_length > max_width) {
		// before we remove columns, check if we can just reduce the size of columns
		for (auto &w : widths) {
			if (w > config.max_col_width) {
				auto max_diff = w - config.max_col_width;
				if (total_length - max_diff <= max_width) {
					// if we reduce the size of this column we fit within the limits!
					// reduce the width exactly enough so that the box fits
					w -= total_length - max_width;
					total_length = max_width;
					break;
				} else {
					// reducing the width of this column does not make the result fit
					// reduce the column width by the maximum amount anyway
					w = config.max_col_width;
					total_length -= max_diff;
				}
			}
		}

		if (total_length > max_width) {
			// the total length is still too large
			// we need to remove columns!
			// first, we add 6 characters to the total length
			// this is what we need to add the "..." in the middle
			total_length += 3 + config.DOTDOTDOT_LENGTH;
			// now select columns to prune
			// we select columns in zig-zag order starting from the middle
			// e.g. if we have 10 columns, we remove #5, then #4, then #6, then #3, then #7, etc
			int64_t offset = 0;
			while (total_length > max_width) {
				auto c = NumericCast<idx_t>(NumericCast<int64_t>(column_count) / 2 + offset);
				total_length -= widths[c] + 3;
				pruned_columns.insert(c);
				if (offset >= 0) {
					offset = -offset - 1;
				} else {
					offset = -offset;
				}
			}
		}
	}

	bool added_split_column = false;
	vector<idx_t> new_widths;
	for (idx_t c = 0; c < column_count; c++) {
		if (pruned_columns.find(c) == pruned_columns.end()) {
			column_map.push_back(c);
			new_widths.push_back(widths[c]);
		} else {
			if (!added_split_column) {
				// "..."
				column_map.push_back(SPLIT_COLUMN);
				new_widths.push_back(config.DOTDOTDOT_LENGTH);
				added_split_column = true;
			}
		}
	}
	return new_widths;
}

void BoxRenderer::RenderHeader(const vector<string> &names, const vector<LogicalType> &result_types,
                               const vector<idx_t> &column_map, const vector<idx_t> &widths,
                               const vector<idx_t> &boundaries, idx_t total_length, bool has_results,
                               BaseResultRenderer &ss) {
	auto column_count = column_map.size();
	// render the top line
	ss << config.LTCORNER;
	idx_t column_index = 0;
	for (idx_t k = 0; k < total_length - 2; k++) {
		if (column_index + 1 < column_count && k == boundaries[column_index]) {
			ss << config.TMIDDLE;
			column_index++;
		} else {
			ss << config.HORIZONTAL;
		}
	}
	ss << config.RTCORNER;
	ss << '\n';

	// render the header names
	for (idx_t c = 0; c < column_count; c++) {
		auto column_idx = column_map[c];
		string name;
		ResultRenderType render_mode;
		if (column_idx == SPLIT_COLUMN) {
			render_mode = ResultRenderType::LAYOUT;
			name = config.DOTDOTDOT;
		} else {
			render_mode = ResultRenderType::COLUMN_NAME;
			name = ConvertRenderValue(names[column_idx]);
		}
		RenderValue(ss, name, widths[c], render_mode);
	}
	ss << config.VERTICAL;
	ss << '\n';

	// render the types
	if (config.render_mode == RenderMode::ROWS) {
		for (idx_t c = 0; c < column_count; c++) {
			auto column_idx = column_map[c];
			string type;
			ResultRenderType render_mode;
			if (column_idx == SPLIT_COLUMN) {
				render_mode = ResultRenderType::LAYOUT;
			} else {
				render_mode = ResultRenderType::COLUMN_TYPE;
				type = RenderType(result_types[column_idx]);
			}
			RenderValue(ss, type, widths[c], render_mode);
		}
		ss << config.VERTICAL;
		ss << '\n';
	}

	// render the line under the header
	ss << config.LMIDDLE;
	column_index = 0;
	for (idx_t k = 0; k < total_length - 2; k++) {
		if (column_index + 1 < column_count && k == boundaries[column_index]) {
			ss << (has_results ? config.MIDDLE : config.DMIDDLE);
			column_index++;
		} else {
			ss << config.HORIZONTAL;
		}
	}
	ss << config.RMIDDLE;
	ss << '\n';
}

void BoxRenderer::RenderValues(const list<ColumnDataCollection> &collections, const vector<idx_t> &column_map,
                               const vector<idx_t> &widths, const vector<LogicalType> &result_types,
                               BaseResultRenderer &ss) {
	auto &top_collection = collections.front();
	auto &bottom_collection = collections.back();
	// render the top rows
	auto top_rows = top_collection.Count();
	auto bottom_rows = bottom_collection.Count();
	auto column_count = column_map.size();

	bool large_number_footer = config.large_number_rendering == LargeNumberRendering::FOOTER;
	vector<ValueRenderAlignment> alignments;
	if (config.render_mode == RenderMode::ROWS) {
		for (idx_t c = 0; c < column_count; c++) {
			auto column_idx = column_map[c];
			if (column_idx == SPLIT_COLUMN) {
				alignments.push_back(ValueRenderAlignment::MIDDLE);
			} else if (large_number_footer && result_types[column_idx].IsNumeric()) {
				alignments.push_back(ValueRenderAlignment::MIDDLE);
			} else {
				alignments.push_back(TypeAlignment(result_types[column_idx]));
			}
		}
	}

	auto rows = top_collection.GetRows();
	for (idx_t r = 0; r < top_rows; r++) {
		for (idx_t c = 0; c < column_count; c++) {
			auto column_idx = column_map[c];
			string str;
			ResultRenderType render_mode;
			if (column_idx == SPLIT_COLUMN) {
				str = config.DOTDOTDOT;
				render_mode = ResultRenderType::LAYOUT;
			} else {
				str = GetRenderValue(ss, rows, column_idx, r, result_types[column_idx], render_mode);
			}
			ValueRenderAlignment alignment;
			if (config.render_mode == RenderMode::ROWS) {
				alignment = alignments[c];
				if (large_number_footer && r == 1) {
					// render readable numbers with highlighting of a NULL value
					render_mode = ResultRenderType::NULL_VALUE;
				}
			} else {
				switch (c) {
				case 0:
					render_mode = ResultRenderType::COLUMN_NAME;
					break;
				case 1:
					render_mode = ResultRenderType::COLUMN_TYPE;
					break;
				default:
					render_mode = ResultRenderType::VALUE;
					break;
				}
				if (c < 2) {
					alignment = ValueRenderAlignment::LEFT;
				} else if (c == SPLIT_COLUMN) {
					alignment = ValueRenderAlignment::MIDDLE;
				} else {
					alignment = ValueRenderAlignment::RIGHT;
				}
			}
			RenderValue(ss, str, widths[c], render_mode, alignment);
		}
		ss << config.VERTICAL;
		ss << '\n';
	}

	if (bottom_rows > 0) {
		if (config.render_mode == RenderMode::COLUMNS) {
			throw InternalException("Columns render mode does not support bottom rows");
		}
		// render the bottom rows
		// first render the divider
		auto brows = bottom_collection.GetRows();
		for (idx_t k = 0; k < 3; k++) {
			for (idx_t c = 0; c < column_count; c++) {
				auto column_idx = column_map[c];
				string str;
				auto alignment = alignments[c];
				if (alignment == ValueRenderAlignment::MIDDLE || column_idx == SPLIT_COLUMN) {
					str = config.DOT;
				} else {
					// align the dots in the center of the column
					ResultRenderType render_mode;
					auto top_value =
					    GetRenderValue(ss, rows, column_idx, top_rows - 1, result_types[column_idx], render_mode);
					auto bottom_value =
					    GetRenderValue(ss, brows, column_idx, bottom_rows - 1, result_types[column_idx], render_mode);
					auto top_length = MinValue<idx_t>(widths[c], Utf8Proc::RenderWidth(top_value));
					auto bottom_length = MinValue<idx_t>(widths[c], Utf8Proc::RenderWidth(bottom_value));
					auto dot_length = MinValue<idx_t>(top_length, bottom_length);
					if (top_length == 0) {
						dot_length = bottom_length;
					} else if (bottom_length == 0) {
						dot_length = top_length;
					}
					if (dot_length > 1) {
						auto padding = dot_length - 1;
						idx_t left_padding, right_padding;
						switch (alignment) {
						case ValueRenderAlignment::LEFT:
							left_padding = padding / 2;
							right_padding = padding - left_padding;
							break;
						case ValueRenderAlignment::RIGHT:
							right_padding = padding / 2;
							left_padding = padding - right_padding;
							break;
						default:
							throw InternalException("Unrecognized value renderer alignment");
						}
						str = string(left_padding, ' ') + config.DOT + string(right_padding, ' ');
					} else {
						if (dot_length == 0) {
							// everything is empty
							alignment = ValueRenderAlignment::MIDDLE;
						}
						str = config.DOT;
					}
				}
				RenderValue(ss, str, widths[c], ResultRenderType::LAYOUT, alignment);
			}
			ss << config.VERTICAL;
			ss << '\n';
		}
		// note that the bottom rows are in reverse order
		for (idx_t r = 0; r < bottom_rows; r++) {
			for (idx_t c = 0; c < column_count; c++) {
				auto column_idx = column_map[c];
				string str;
				ResultRenderType render_mode;
				if (column_idx == SPLIT_COLUMN) {
					str = config.DOTDOTDOT;
					render_mode = ResultRenderType::LAYOUT;
				} else {
					str = GetRenderValue(ss, brows, column_idx, bottom_rows - r - 1, result_types[column_idx],
					                     render_mode);
				}
				RenderValue(ss, str, widths[c], render_mode, alignments[c]);
			}
			ss << config.VERTICAL;
			ss << '\n';
		}
	}
}

void BoxRenderer::RenderRowCount(string row_count_str, string shown_str, const string &column_count_str,
                                 const vector<idx_t> &boundaries, bool has_hidden_rows, bool has_hidden_columns,
                                 idx_t total_length, idx_t row_count, idx_t column_count, idx_t minimum_row_length,
                                 BaseResultRenderer &ss) {
	// check if we can merge the row_count_str and the shown_str
	bool display_shown_separately = has_hidden_rows;
	if (has_hidden_rows && total_length >= row_count_str.size() + shown_str.size() + 5) {
		// we can!
		row_count_str += " " + shown_str;
		shown_str = string();
		display_shown_separately = false;
		minimum_row_length = row_count_str.size() + 4;
	}
	auto minimum_length = row_count_str.size() + column_count_str.size() + 6;
	bool render_rows_and_columns = total_length >= minimum_length &&
	                               ((has_hidden_columns && row_count > 0) || (row_count >= 10 && column_count > 1));
	bool render_rows = total_length >= minimum_row_length && (row_count == 0 || row_count >= 10);
	bool render_anything = true;
	if (!render_rows && !render_rows_and_columns) {
		render_anything = false;
	}
	// render the bottom of the result values, if there are any
	if (row_count > 0) {
		ss << (render_anything ? config.LMIDDLE : config.LDCORNER);
		idx_t column_index = 0;
		for (idx_t k = 0; k < total_length - 2; k++) {
			if (column_index + 1 < boundaries.size() && k == boundaries[column_index]) {
				ss << config.DMIDDLE;
				column_index++;
			} else {
				ss << config.HORIZONTAL;
			}
		}
		ss << (render_anything ? config.RMIDDLE : config.RDCORNER);
		ss << '\n';
	}
	if (!render_anything) {
		return;
	}

	if (render_rows_and_columns) {
		ss << config.VERTICAL;
		ss << " ";
		ss.Render(ResultRenderType::FOOTER, row_count_str);
		ss << string(total_length - row_count_str.size() - column_count_str.size() - 4, ' ');
		ss.Render(ResultRenderType::FOOTER, column_count_str);
		ss << " ";
		ss << config.VERTICAL;
		ss << '\n';
	} else if (render_rows) {
		RenderValue(ss, row_count_str, total_length - 4, ResultRenderType::FOOTER);
		ss << config.VERTICAL;
		ss << '\n';

		if (display_shown_separately) {
			RenderValue(ss, shown_str, total_length - 4, ResultRenderType::FOOTER);
			ss << config.VERTICAL;
			ss << '\n';
		}
	}
	// render the bottom line
	ss << config.LDCORNER;
	for (idx_t k = 0; k < total_length - 2; k++) {
		ss << config.HORIZONTAL;
	}
	ss << config.RDCORNER;
	ss << '\n';
}

void BoxRenderer::Render(ClientContext &context, const vector<string> &names, const ColumnDataCollection &result,
                         BaseResultRenderer &ss) {
	if (result.ColumnCount() != names.size()) {
		throw InternalException("Error in BoxRenderer::Render - unaligned columns and names");
	}
	auto max_width = config.max_width;
	if (max_width == 0) {
		if (Printer::IsTerminal(OutputStream::STREAM_STDOUT)) {
			max_width = Printer::TerminalWidth();
		} else {
			max_width = 120;
		}
	}
	// we do not support max widths under 80
	max_width = MaxValue<idx_t>(80, max_width);

	// figure out how many/which rows to render
	idx_t row_count = result.Count();
	idx_t rows_to_render = MinValue<idx_t>(row_count, config.max_rows);
	if (row_count <= config.max_rows + 3) {
		// hiding rows adds 3 extra rows
		// so hiding rows makes no sense if we are only slightly over the limit
		// if we are 1 row over the limit hiding rows will actually increase the number of lines we display!
		// in this case render all the rows
		rows_to_render = row_count;
	}
	idx_t top_rows;
	idx_t bottom_rows;
	if (rows_to_render == row_count) {
		top_rows = row_count;
		bottom_rows = 0;
	} else {
		top_rows = rows_to_render / 2 + (rows_to_render % 2 != 0 ? 1 : 0);
		bottom_rows = rows_to_render - top_rows;
	}
	auto row_count_str = to_string(row_count) + " rows";
	bool has_limited_rows = config.limit > 0 && row_count == config.limit;
	if (has_limited_rows) {
		row_count_str = "? rows";
	}
	string shown_str;
	bool has_hidden_rows = top_rows < row_count;
	if (has_hidden_rows) {
		shown_str = "(";
		if (has_limited_rows) {
			shown_str += ">" + to_string(config.limit - 1) + " rows, ";
		}
		shown_str += to_string(top_rows + bottom_rows) + " shown)";
	}
	auto minimum_row_length = MaxValue<idx_t>(row_count_str.size(), shown_str.size()) + 4;

	// fetch the top and bottom render collections from the result
	auto collections = FetchRenderCollections(context, result, top_rows, bottom_rows);
	auto column_names = names;
	auto result_types = result.Types();
	if (config.render_mode == RenderMode::COLUMNS) {
		collections = PivotCollections(context, std::move(collections), column_names, result_types, row_count);
	}

	// for each column, figure out the width
	// start off by figuring out the name of the header by looking at the column name and column type
	idx_t min_width = has_hidden_rows || row_count == 0 ? minimum_row_length : 0;
	vector<idx_t> column_map;
	idx_t total_length;
	auto widths =
	    ComputeRenderWidths(column_names, result_types, collections, min_width, max_width, column_map, total_length);

	// render boundaries for the individual columns
	vector<idx_t> boundaries;
	for (idx_t c = 0; c < widths.size(); c++) {
		idx_t render_boundary;
		if (c == 0) {
			render_boundary = widths[c] + 2;
		} else {
			render_boundary = boundaries[c - 1] + widths[c] + 3;
		}
		boundaries.push_back(render_boundary);
	}

	// now begin rendering
	// first render the header
	RenderHeader(column_names, result_types, column_map, widths, boundaries, total_length, row_count > 0, ss);

	// render the values, if there are any
	RenderValues(collections, column_map, widths, result_types, ss);

	// render the row count and column count
	auto column_count_str = to_string(result.ColumnCount()) + " column";
	if (result.ColumnCount() > 1) {
		column_count_str += "s";
	}
	bool has_hidden_columns = false;
	for (auto entry : column_map) {
		if (entry == SPLIT_COLUMN) {
			has_hidden_columns = true;
			break;
		}
	}
	idx_t column_count = column_map.size();
	if (config.render_mode == RenderMode::COLUMNS) {
		if (has_hidden_columns) {
			has_hidden_rows = true;
			shown_str = " (" + to_string(column_count - 3) + " shown)";
		} else {
			shown_str = string();
		}
	} else {
		if (has_hidden_columns) {
			column_count--;
			column_count_str += " (" + to_string(column_count) + " shown)";
		}
	}

	RenderRowCount(std::move(row_count_str), std::move(shown_str), column_count_str, boundaries, has_hidden_rows,
	               has_hidden_columns, total_length, row_count, column_count, minimum_row_length, ss);
}

} // namespace duckdb
