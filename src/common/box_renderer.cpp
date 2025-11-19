#include "duckdb/common/box_renderer.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/original/std/sstream.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {
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
	case ResultRenderType::STRING_LITERAL:
		RenderStringLiteral(val, value_type);
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
// Box Renderer Implementation
//===--------------------------------------------------------------------===//
struct HighlightingAnnotation {
	HighlightingAnnotation(ResultRenderType render_mode, idx_t start) : render_mode(render_mode), start(start) {
	}

	ResultRenderType render_mode;
	idx_t start;
};

struct BoxRenderValue {
	BoxRenderValue(string text_p, ResultRenderType render_mode, ValueRenderAlignment alignment,
	               LogicalType type_p = LogicalTypeId::INVALID, optional_idx render_width = optional_idx())
	    : text(std::move(text_p)), render_mode(render_mode), alignment(alignment), type(std::move(type_p)),
	      render_width(render_width) {
	}

	string text;
	ResultRenderType render_mode;
	vector<HighlightingAnnotation> annotations;
	ValueRenderAlignment alignment;
	LogicalType type;
	optional_idx render_width;
	bool decomposed = false;
};

enum class RenderRowType { ROW_VALUES, SEPARATOR, DIVIDER, FOOTER };

struct BoxRendererFooter {
	string row_count_str;
	string readable_rows_str;
	string shown_str;
	string column_count_str;
	idx_t render_length = 0;
	bool must_show_footer = false;
	bool show_footer = true;
	bool has_hidden_rows = false;
	bool has_hidden_columns = false;
};

struct BoxRenderRow {
	BoxRenderRow(RenderRowType row_type = RenderRowType::ROW_VALUES) // NOLINT: allow implicit conversion
	    : row_type(row_type) {
	}

	RenderRowType row_type;
	vector<BoxRenderValue> values;
};

struct BoxRendererImplementation {
	BoxRendererImplementation(BoxRendererConfig &config, ClientContext &context, const vector<string> &names,
	                          const ColumnDataCollection &result, BaseResultRenderer &ss);

public:
	void Render();

private:
	BoxRendererConfig &config;
	ClientContext &context;
	vector<string> column_names;
	vector<LogicalType> result_types;
	const ColumnDataCollection &result;
	BaseResultRenderer &ss;
	vector<idx_t> column_widths;
	vector<idx_t> column_boundary_positions;
	idx_t total_render_length = 0;
	vector<BoxRenderRow> render_rows;
	BoxRendererFooter footer;

private:
	void RenderValue(const string &value, idx_t column_width, ResultRenderType render_mode,
	                 const vector<HighlightingAnnotation> &annotations,
	                 ValueRenderAlignment alignment = ValueRenderAlignment::MIDDLE,
	                 optional_idx render_width = optional_idx());
	string RenderType(const LogicalType &type);
	ValueRenderAlignment TypeAlignment(const LogicalType &type);
	string GetRenderValue(ColumnDataRowCollection &rows, idx_t c, idx_t r, const LogicalType &type,
	                      ResultRenderType &render_mode);
	list<ColumnDataCollection> FetchRenderCollections(const ColumnDataCollection &result, idx_t top_rows,
	                                                  idx_t bottom_rows);
	list<ColumnDataCollection> PivotCollections(list<ColumnDataCollection> input, idx_t row_count);
	void ComputeRenderWidths(list<ColumnDataCollection> &collections, idx_t min_width, idx_t max_width);
	void RenderValues();
	void UpdateColumnCountFooter(idx_t column_count, const unordered_set<idx_t> &pruned_columns);
	string TruncateValue(const string &value, idx_t column_width, idx_t &pos, idx_t &current_render_width);

	void ComputeRowFooter(idx_t row_count, idx_t rendered_rows);
	void RenderFooter(idx_t row_count, idx_t column_count);

	string FormatNumber(const string &input);
	string ConvertRenderValue(const string &input, const LogicalType &type);
	string ConvertRenderValue(const string &input);
	void RenderLayoutLine(const char *layout, const char *boundary, const char *left_corner, const char *right_corner);
	//! Try to format a large number in a readable way (e.g. 1234567 -> 1.23 million)
	string TryFormatLargeNumber(const string &numeric);

	bool CanPrettyPrint(const LogicalType &type);
	bool CanHighlight(const LogicalType &type);
	void PrettyPrintValue(BoxRenderValue &render_value, idx_t max_rows, idx_t max_width);
	void HighlightValue(BoxRenderValue &render_value);
};

BoxRendererImplementation::BoxRendererImplementation(BoxRendererConfig &config, ClientContext &context,
                                                     const vector<string> &names, const ColumnDataCollection &result,
                                                     BaseResultRenderer &ss)
    : config(config), context(context), column_names(names), result(result), ss(ss) {
	result_types = result.Types();
}

void BoxRendererImplementation::ComputeRowFooter(idx_t row_count, idx_t rendered_rows) {
	footer.column_count_str = to_string(result.ColumnCount()) + " column";
	if (result.ColumnCount() > 1) {
		footer.column_count_str += "s";
	}
	footer.row_count_str = FormatNumber(to_string(row_count)) + " rows";
	bool has_limited_rows = config.limit > 0 && row_count == config.limit;
	if (has_limited_rows) {
		footer.row_count_str = "? rows";
	}
	if (config.large_number_rendering == LargeNumberRendering::FOOTER && !has_limited_rows) {
		footer.readable_rows_str = TryFormatLargeNumber(to_string(row_count));
		if (!footer.readable_rows_str.empty()) {
			footer.readable_rows_str += " rows";
		}
	}
	footer.has_hidden_rows = rendered_rows < row_count;
	if (footer.has_hidden_rows) {
		if (has_limited_rows) {
			footer.shown_str += ">" + FormatNumber(to_string(config.limit - 1)) + " rows, ";
		}
		footer.shown_str += FormatNumber(to_string(rendered_rows)) + " shown";
	}
	footer.must_show_footer = has_limited_rows || footer.has_hidden_rows || row_count == 0;
	footer.render_length = MaxValue<idx_t>(MaxValue<idx_t>(footer.row_count_str.size(), footer.shown_str.size() + 2),
	                                       footer.readable_rows_str.size() + 2) +
	                       4;
}

void BoxRendererImplementation::UpdateColumnCountFooter(idx_t column_count,
                                                        const unordered_set<idx_t> &pruned_columns) {
	if (pruned_columns.empty()) {
		// no pruned columns - no need to update the footer
		return;
	}
	if (config.render_mode == RenderMode::COLUMNS) {
		// in columns mode - pruned columns really means pruned rows
		footer.has_hidden_rows = true;
		idx_t shown_row_count = column_count - pruned_columns.size();
		footer.shown_str = to_string(shown_row_count - 2) + " shown";
	} else {
		footer.has_hidden_columns = true;
		idx_t shown_column_count = column_count - pruned_columns.size();
		footer.column_count_str += " (" + to_string(shown_column_count) + " shown)";
	}
}

void BoxRendererImplementation::Render() {
	if (result.ColumnCount() != column_names.size()) {
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
	ComputeRowFooter(row_count, top_rows + bottom_rows);

	// fetch the top and bottom render collections from the result
	auto collections = FetchRenderCollections(result, top_rows, bottom_rows);
	if (config.render_mode == RenderMode::COLUMNS && rows_to_render > 0) {
		collections = PivotCollections(std::move(collections), row_count);
	}

	// for each column, figure out the width
	// start off by figuring out the name of the header by looking at the column name and column type
	idx_t min_width = footer.must_show_footer ? footer.render_length : 0;
	ComputeRenderWidths(collections, min_width, max_width);

	// render boundaries for the individual columns
	for (idx_t c = 0; c < column_widths.size(); c++) {
		idx_t render_boundary;
		if (c == 0) {
			render_boundary = column_widths[c] + 2;
		} else {
			render_boundary = column_boundary_positions[c - 1] + column_widths[c] + 3;
		}
		column_boundary_positions.push_back(render_boundary);
	}

	// now begin rendering
	// render the box
	RenderValues();

	// render the row count and column count
	idx_t column_count = result_types.size();
	RenderFooter(row_count, column_count);
}

string BoxRenderer::TruncateValue(const string &value, idx_t column_width, idx_t &pos, idx_t &current_render_width) {
	idx_t start_pos = pos;
	while (pos < value.size()) {
		if (value[pos] == '\n') {
			// newline character - stop rendering for this line - but skip the newline
			idx_t render_pos = pos;
			pos++;
			return value.substr(start_pos, render_pos - start_pos);
		}
		// check if this character fits...
		auto char_size = Utf8Proc::RenderWidth(value.c_str(), value.size(), pos);
		if (current_render_width + char_size > column_width) {
			// it doesn't! stop
			break;
		}
		// it does! move to the next character
		current_render_width += char_size;
		pos = Utf8Proc::NextGraphemeCluster(value.c_str(), value.size(), pos);
	}
	return value.substr(start_pos, pos - start_pos);
}

string BoxRendererImplementation::TruncateValue(const string &value, idx_t column_width, idx_t &pos,
                                                idx_t &current_render_width) {
	return BoxRenderer::TruncateValue(value, column_width, pos, current_render_width);
}

void BoxRendererImplementation::RenderValue(const string &value, idx_t column_width, ResultRenderType render_mode,
                                            const vector<HighlightingAnnotation> &annotations,
                                            ValueRenderAlignment alignment, optional_idx render_width_input) {
	idx_t render_width;
	if (render_width_input.IsValid()) {
		render_width = render_width_input.GetIndex();
		if (render_width != Utf8Proc::RenderWidth(value)) {
			throw InternalException("Misaligned render width provided for string \"%s\"", value);
		}
	} else {
		render_width = Utf8Proc::RenderWidth(value);
	}

	const_reference<string> render_value(value);
	string small_value;
	idx_t max_render_pos = value.size();
	if (render_width > column_width) {
		// the string is too large to fit in this column!
		// the size of this column must have been reduced
		// figure out how much of this value we can render
		idx_t pos = 0;
		idx_t current_render_width = config.DOTDOTDOT_LENGTH;
		small_value = TruncateValue(value, column_width, pos, current_render_width);
		max_render_pos = small_value.size();
		small_value += config.DOTDOTDOT;
		render_width = current_render_width;
		render_value = const_reference<string>(small_value);
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
	if (!annotations.empty()) {
		// if we have annotations split up the rendering between annotations
		idx_t pos = 0;
		ResultRenderType active_render_mode = render_mode;
		for (auto &annotation : annotations) {
			if (annotation.start >= max_render_pos) {
				break;
			}
			auto render_end = MinValue<idx_t>(max_render_pos, annotation.start);
			ss.Render(active_render_mode, render_value.get().substr(pos, render_end - pos));
			active_render_mode = annotation.render_mode;
			pos = render_end;
		}
		if (pos < render_value.get().size()) {
			ss.Render(active_render_mode, render_value.get().substr(pos, render_value.get().size() - pos));
		}
	} else {
		ss.Render(render_mode, render_value.get());
	}
	ss << string(rpadding, ' ');
}

string BoxRendererImplementation::RenderType(const LogicalType &type) {
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

ValueRenderAlignment BoxRendererImplementation::TypeAlignment(const LogicalType &type) {
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

string BoxRenderer::TryFormatLargeNumber(const string &numeric, char decimal_sep) {
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
	result += decimal_sep == '\0' ? '.' : decimal_sep;
	result += decimal_str.substr(decimal_str.size() - 2, 2);
	result += " ";
	result += unit;
	return result;
}

string BoxRendererImplementation::TryFormatLargeNumber(const string &numeric) {
	return BoxRenderer::TryFormatLargeNumber(numeric, config.decimal_separator);
}

list<ColumnDataCollection> BoxRendererImplementation::FetchRenderCollections(const ColumnDataCollection &result,
                                                                             idx_t top_rows, idx_t bottom_rows) {
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
			} else {
				config.large_number_rendering = LargeNumberRendering::NONE;
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

list<ColumnDataCollection> BoxRendererImplementation::PivotCollections(list<ColumnDataCollection> input,
                                                                       idx_t row_count) {
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

string BoxRendererImplementation::ConvertRenderValue(const string &input) {
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

string BoxRendererImplementation::FormatNumber(const string &input) {
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

string BoxRendererImplementation::ConvertRenderValue(const string &input, const LogicalType &type) {
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

string BoxRendererImplementation::GetRenderValue(ColumnDataRowCollection &rows, idx_t c, idx_t r,
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

struct JSONParser {
public:
	virtual ~JSONParser() = default;

protected:
	enum class JSONState { REGULAR, IN_QUOTE, ESCAPE };

	struct Separator {
		Separator(char sep) // NOLINT: allow implicit conversion
		    : sep(sep), inlined(false) {
		}

		char sep;
		bool inlined = false;
	};

public:
	bool Process(const string &value);

protected:
	virtual void HandleNull() {
	}
	virtual void HandleBracketOpen(char bracket) {
	}
	virtual void HandleBracketClose(char bracket) {
	}
	virtual void HandleQuoteStart(char quote) {
	}
	virtual void HandleQuoteEnd(char quote) {
	}
	virtual void HandleComma(char comma) {
	}
	virtual void HandleColon() {
	}
	virtual void HandleCharacter(char c) {
	}
	virtual void HandleEscapeStart(char c) {
	}
	virtual void Finish() {
	}

protected:
	bool SeparatorIsMatching(Separator &sep, char closing_sep);
	idx_t Depth() const {
		return separators.size();
	}

protected:
	JSONState state = JSONState::REGULAR;
	vector<Separator> separators;
	idx_t pos = 0;
	bool success = true;
};

bool JSONParser::SeparatorIsMatching(Separator &sep, char closing_sep) {
	if (sep.sep == '{' && closing_sep == '}') {
		return true;
	}
	if (sep.sep == '[' && closing_sep == ']') {
		return true;
	}
	return false;
}

bool IsWhitespaceEscape(const char c) {
	// \n and \t are whitespace escapes
	return c == 'n' || c == 't';
}

bool JSONParser::Process(const string &value) {
	separators.clear();
	state = JSONState::REGULAR;
	char quote_char = '"';
	bool can_parse_value = false;
	pos = 0;
	for (; success && pos < value.size(); pos++) {
		auto c = value[pos];
		if (state == JSONState::REGULAR) {
			if (can_parse_value) {
				// check if this is "null"
				if (pos + 4 < value.size() && StringUtil::CharacterToLower(c) == 'n' &&
				    StringUtil::CharacterToLower(value[pos + 1]) == 'u' &&
				    StringUtil::CharacterToLower(value[pos + 2]) == 'l' &&
				    StringUtil::CharacterToLower(value[pos + 3]) == 'l') {
					HandleNull();
					pos += 3;
					continue;
				}
			}
			switch (c) {
			case '[':
			case '{': {
				// add a newline and indentation based on the separator count
				separators.push_back(c);
				HandleBracketOpen(c);
				can_parse_value = c == '[';
				break;
			}
			case '}':
			case ']': {
				// closing bracket - move to next line and pop back the separator
				if (separators.empty() || !SeparatorIsMatching(separators.back(), c)) {
					throw InternalException("Failed to parse JSON string %s - invalid JSON", value);
				}
				separators.pop_back();
				HandleBracketClose(c);
				break;
			}
			case '"':
			case '\'':
				HandleQuoteStart(c);
				quote_char = c;
				state = JSONState::IN_QUOTE;
				break;
			case ',':
				// comma - move to next line
				HandleComma(c);
				break;
			case ':':
				HandleColon();
				can_parse_value = true;
				break;
			case '\\':
				// skip literal "\n" and "\t" (these were escaped previously by our rendering algorithm)
				if (pos + 1 < value.size() && IsWhitespaceEscape(value[pos + 1])) {
					pos++;
					break;
				}
				// if this is not a whitespace escape just handle it
				HandleCharacter(c);
				break;
			case ' ':
			case '\t':
			case '\n':
				// skip whitespace
				break;
			default:
				HandleCharacter(c);
				break;
			}
		} else if (state == JSONState::IN_QUOTE) {
			if (c == quote_char) {
				// break out of quotes
				state = JSONState::REGULAR;
				HandleQuoteEnd(c);
			} else if (c == '\\') {
				// escape
				state = JSONState::ESCAPE;
				HandleEscapeStart(c);
			} else {
				HandleCharacter(c);
			}
		} else if (state == JSONState::ESCAPE) {
			state = JSONState::IN_QUOTE;
			HandleCharacter(c);
		} else {
			throw InternalException("Invalid json state");
		}
	}
	if (!success) {
		return false;
	}
	Finish();
	return true;
}

enum class JSONFormattingMode { STANDARD, COMPACT_VERTICAL, COMPACT_HORIZONTAL };

enum class JSONComponentType { BRACKET_OPEN, BRACKET_CLOSE, LITERAL, COLON, COMMA, NULL_VALUE };

enum class JSONFormattingResult { SUCCESS, TOO_MANY_ROWS, TOO_WIDE };

struct JSONComponent {
	JSONComponent(JSONComponentType type, string text_p) : type(type), text(std::move(text_p)) {
	}

	JSONComponentType type;
	string text;
};

struct JSONFormatter : public JSONParser {
public:
	explicit JSONFormatter() {
	}

	static void FormatValue(BoxRenderValue &render_value, idx_t max_rows, idx_t max_width) {
		// process the components
		JSONFormatter formatter;
		formatter.Process(render_value.text);

		idx_t indentation_size = 2;

		auto result =
		    formatter.TryFormat(JSONFormattingMode::STANDARD, render_value, max_rows, max_width, indentation_size);
		if (result == JSONFormattingResult::SUCCESS) {
			return;
		}
		// if we exceeded the max row count - try in compact mode
		JSONFormattingMode mode;
		if (result == JSONFormattingResult::TOO_WIDE) {
			// reduce indentation size if the result was too wide
			mode = JSONFormattingMode::COMPACT_HORIZONTAL;
			indentation_size = 1;
		} else {
			mode = JSONFormattingMode::COMPACT_VERTICAL;
		}
		result = formatter.TryFormat(mode, render_value, max_rows, max_width, indentation_size);
		if (result == JSONFormattingResult::SUCCESS) {
			return;
		}
	}

protected:
	void HandleNull() override {
		components.emplace_back(JSONComponentType::NULL_VALUE, "null");
	}

	void HandleBracketOpen(char bracket) override {
		components.emplace_back(JSONComponentType::BRACKET_OPEN, string(1, bracket));
	}

	void HandleBracketClose(char bracket) override {
		components.emplace_back(JSONComponentType::BRACKET_CLOSE, string(1, bracket));
	}

	void HandleQuoteStart(char quote) override {
		AddLiteralCharacter(quote);
	}

	void HandleQuoteEnd(char quote) override {
		AddLiteralCharacter(quote);
	}

	void HandleComma(char comma) override {
		components.emplace_back(JSONComponentType::COMMA, ",");
	}

	void HandleColon() override {
		components.emplace_back(JSONComponentType::COLON, ":");
	}

	void HandleCharacter(char c) override {
		AddLiteralCharacter(c);
	}

	void HandleEscapeStart(char c) override {
		AddLiteralCharacter(c);
	}

	void AddLiteralCharacter(char c) {
		if (components.empty() || components.back().type != JSONComponentType::LITERAL) {
			components.emplace_back(JSONComponentType::LITERAL, "");
		}
		components.back().text += c;
	}

	struct FormatState {
		JSONFormattingMode mode;
		string result;
		idx_t component_idx = 0;
		idx_t row_count = 0;
		idx_t line_length = 0;
		idx_t depth = 0;
		idx_t max_rows;
		idx_t max_width;
		idx_t indentation_size = 2;
		JSONFormattingResult format_result = JSONFormattingResult::SUCCESS;
	};

	bool LiteralFits(FormatState &format_state, idx_t render_width) {
		auto &line_length = format_state.line_length;
		if (line_length + render_width > format_state.max_width) {
			return false;
		}
		return true;
	}

	bool LiteralFits(FormatState &format_state, const string &text) {
		idx_t render_width = Utf8Proc::RenderWidth(text);
		return LiteralFits(format_state, render_width);
	}

	void AddLiteral(FormatState &format_state, const string &text, bool skip_adding_if_does_not_fit = false) {
		auto &result = format_state.result;
		auto &line_length = format_state.line_length;
		idx_t render_width = Utf8Proc::RenderWidth(text);
		if (!LiteralFits(format_state, render_width)) {
			if (skip_adding_if_does_not_fit) {
				return;
			}
			AddNewline(format_state);
			if (format_state.format_result != JSONFormattingResult::SUCCESS) {
				return;
			}
		}
		result += text;
		line_length += render_width;
		if (line_length > format_state.max_width) {
			format_state.format_result = JSONFormattingResult::TOO_WIDE;
		}
	}
	void AddSpace(FormatState &format_state) {
		AddLiteral(format_state, " ", true);
	}
	void AddNewline(FormatState &format_state) {
		auto &result = format_state.result;
		auto &depth = format_state.depth;
		auto &row_count = format_state.row_count;
		auto &line_length = format_state.line_length;
		result += '\n';
		result += string(depth, ' ');
		row_count++;
		if (row_count > format_state.max_rows) {
			format_state.format_result = JSONFormattingResult::TOO_MANY_ROWS;
			return;
		}
		line_length = depth;
		if (line_length > format_state.max_width) {
			format_state.format_result = JSONFormattingResult::TOO_WIDE;
		}
	}

	enum class InlineMode { STANDARD, INLINED_SINGLE_LINE, INLINED_MULTI_LINE };

	void FormatComponent(FormatState &format_state, JSONComponent &component, InlineMode inline_mode) {
		auto &depth = format_state.depth;
		auto &line_length = format_state.line_length;
		auto &max_width = format_state.max_width;
		auto &c = format_state.component_idx;
		switch (component.type) {
		case JSONComponentType::BRACKET_OPEN: {
			depth += component.text == "{" ? format_state.indentation_size : 1;
			AddLiteral(format_state, component.text);
			if (inline_mode == InlineMode::STANDARD) {
				// not inlined
				// look forward until the corresponding bracket open - can we inline and not exceed the column width?
				idx_t peek_depth = 0;
				idx_t render_size = line_length;
				idx_t peek_idx;
				InlineMode inline_child_mode = InlineMode::STANDARD;
				for (peek_idx = c + 1; peek_idx < components.size() && render_size <= max_width; peek_idx++) {
					auto &peek_component = components[peek_idx];
					if (peek_component.type == JSONComponentType::BRACKET_OPEN) {
						peek_depth++;
					} else if (peek_component.type == JSONComponentType::BRACKET_CLOSE) {
						if (peek_depth == 0) {
							// close!
							if (render_size + 1 < max_width) {
								// fits within a single line - inline on a single line
								inline_child_mode = InlineMode::INLINED_SINGLE_LINE;
							}
							break;
						}
						peek_depth--;
					}
					render_size += Utf8Proc::RenderWidth(peek_component.text);
					if (peek_component.type == JSONComponentType::COMMA ||
					    peek_component.type == JSONComponentType::COLON) {
						render_size++;
					}
				}
				if (component.text == "[") {
					// for arrays - we always inline them UNLESS there are complex objects INSIDE of the bracket
					// scan forward until the end of the array to figure out if this is true or not
					for (peek_idx = c + 1; peek_idx < components.size(); peek_idx++) {
						auto &peek_component = components[peek_idx];
						peek_depth = 0;
						if (peek_component.type == JSONComponentType::BRACKET_OPEN) {
							if (peek_component.text == "{") {
								// nested structure within the array
								break;
							}
							peek_depth++;
						}
						if (peek_component.type == JSONComponentType::BRACKET_CLOSE) {
							if (peek_depth == 0) {
								inline_child_mode = InlineMode::INLINED_MULTI_LINE;
								break;
							}
							peek_depth--;
						}
					}
				}
				if (inline_child_mode != InlineMode::STANDARD) {
					// we can inline! do it
					for (idx_t inline_idx = c + 1; inline_idx <= peek_idx; inline_idx++) {
						auto &inline_component = components[inline_idx];
						if (inline_child_mode == InlineMode::INLINED_MULTI_LINE && inline_idx + 1 <= peek_idx) {
							auto &next_component = components[inline_idx + 1];
							if (next_component.type == JSONComponentType::COMMA ||
							    next_component.type == JSONComponentType::BRACKET_CLOSE) {
								if (!LiteralFits(format_state, inline_component.text + next_component.text)) {
									AddNewline(format_state);
								}
							}
						}
						FormatComponent(format_state, inline_component, inline_child_mode);
					}
					c = peek_idx;
					return;
				}
				if (format_state.mode == JSONFormattingMode::COMPACT_VERTICAL) {
					// we can't inline - but is the next token a bracket open?
					if (c + 1 < components.size() && components[c + 1].type == JSONComponentType::BRACKET_OPEN) {
						// it is! that bracket open will add a newline - we don't need to do it here
						return;
					}
				}
				AddNewline(format_state);
			}
			break;
		}
		case JSONComponentType::BRACKET_CLOSE: {
			idx_t depth_diff = component.text == "}" ? format_state.indentation_size : 1;
			if (depth < depth_diff) {
				// shouldn't happen - but guard against underflows
				depth = 0;
			} else {
				depth -= depth_diff;
			}
			if (inline_mode == InlineMode::STANDARD) {
				AddNewline(format_state);
			}
			AddLiteral(format_state, component.text);
			break;
		}
		case JSONComponentType::COMMA:
		case JSONComponentType::COLON:
			AddLiteral(format_state, component.text);
			bool always_inline;
			if (format_state.mode == JSONFormattingMode::COMPACT_HORIZONTAL) {
				// if we are trying to compact horizontally - don't inline colons unless it fits
				always_inline = false;
			} else {
				// in normal processing we always inline colons
				always_inline = component.type == JSONComponentType::COLON;
			}
			if (inline_mode != InlineMode::STANDARD || always_inline) {
				AddSpace(format_state);
			} else {
				if (format_state.mode != JSONFormattingMode::STANDARD) {
					// if we are not inlining in compact mode, try to inline until the next comma
					idx_t peek_depth = 0;
					idx_t render_size = line_length + 1;
					idx_t peek_idx;
					bool inline_comma = false;
					for (peek_idx = c + 1; peek_idx < components.size() && render_size <= max_width; peek_idx++) {
						auto &peek_component = components[peek_idx];
						if (peek_component.type == JSONComponentType::BRACKET_OPEN) {
							peek_depth++;
						} else if (peek_component.type == JSONComponentType::BRACKET_CLOSE) {
							if (peek_depth == 0) {
								inline_comma = render_size + 1 < max_width;
								break;
							}
							peek_depth--;
						}
						if (peek_depth == 0 && peek_component.type == JSONComponentType::COMMA) {
							// found the next comma - inline!
							inline_comma = render_size + 2 <= max_width;
							break;
						}
						render_size += Utf8Proc::RenderWidth(peek_component.text);
						if (peek_component.type == JSONComponentType::COMMA ||
						    peek_component.type == JSONComponentType::COLON) {
							render_size++;
						}
					}
					if (inline_comma) {
						// we can inline until the next comma! do it
						AddSpace(format_state);
						for (idx_t inline_idx = c + 1; inline_idx < peek_idx; inline_idx++) {
							auto &inline_component = components[inline_idx];
							FormatComponent(format_state, inline_component, InlineMode::INLINED_SINGLE_LINE);
						}
						c = peek_idx - 1;
						return;
					}
				}
				AddNewline(format_state);
			}
			break;
		case JSONComponentType::NULL_VALUE:
		case JSONComponentType::LITERAL:
			AddLiteral(format_state, component.text);
			break;
		default:
			throw InternalException("Unsupported JSON component type");
		}
	}

	JSONFormattingResult TryFormat(JSONFormattingMode mode, BoxRenderValue &render_value, idx_t max_rows,
	                               idx_t max_width, idx_t indentation_size = 2) {
		FormatState format_state;
		format_state.mode = mode;
		format_state.max_rows = max_rows;
		format_state.max_width = max_width;
		format_state.indentation_size = indentation_size;
		for (format_state.component_idx = 0; format_state.component_idx < components.size() &&
		                                     format_state.format_result == JSONFormattingResult::SUCCESS;
		     format_state.component_idx++) {
			auto &component = components[format_state.component_idx];
			FormatComponent(format_state, component, InlineMode::STANDARD);
		}

		if (format_state.format_result != JSONFormattingResult::SUCCESS) {
			return format_state.format_result;
		}
		render_value.text = format_state.result;
		return JSONFormattingResult::SUCCESS;
	}

protected:
	vector<JSONComponent> components;
};

struct JSONHighlighter : public JSONParser {
public:
	explicit JSONHighlighter(BoxRenderValue &render_value) : render_value(render_value) {
	}

protected:
	void HandleNull() override {
		render_value.annotations.emplace_back(ResultRenderType::NULL_VALUE, pos);
		render_value.annotations.emplace_back(render_value.render_mode, pos + 4);
	}

	void HandleQuoteStart(char quote) override {
		render_value.annotations.emplace_back(ResultRenderType::STRING_LITERAL, pos);
	}

	void HandleQuoteEnd(char quote) override {
		render_value.annotations.emplace_back(render_value.render_mode, pos + 1);
	}

protected:
	BoxRenderValue &render_value;
};

bool BoxRendererImplementation::CanPrettyPrint(const LogicalType &type) {
	return type.IsJSONType() || type.IsNested();
}

bool BoxRendererImplementation::CanHighlight(const LogicalType &type) {
	return type.IsJSONType() || type.IsNested();
}

void BoxRendererImplementation::PrettyPrintValue(BoxRenderValue &render_value, idx_t max_rows, idx_t max_width) {
	if (!CanPrettyPrint(render_value.type)) {
		return;
	}
	JSONFormatter::FormatValue(render_value, max_rows, max_width);
}

void BoxRendererImplementation::HighlightValue(BoxRenderValue &render_value) {
	if (!CanHighlight(render_value.type)) {
		return;
	}
	JSONHighlighter highlighter(render_value);
	highlighter.Process(render_value.text);
}
void BoxRendererImplementation::ComputeRenderWidths(list<ColumnDataCollection> &collections, idx_t min_width,
                                                    idx_t max_width) {
	auto column_count = result_types.size();

	// prepare all rows for rendering
	// header / type
	BoxRenderRow header_row;
	BoxRenderRow type_row;
	for (idx_t c = 0; c < column_count; c++) {
		header_row.values.emplace_back(ConvertRenderValue(column_names[c]), ResultRenderType::COLUMN_NAME,
		                               ValueRenderAlignment::MIDDLE);
		type_row.values.emplace_back(RenderType(result_types[c]), ResultRenderType::COLUMN_TYPE,
		                             ValueRenderAlignment::MIDDLE);
	}
	render_rows.push_back(std::move(header_row));
	if (config.render_mode == RenderMode::ROWS) {
		render_rows.push_back(std::move(type_row));
	}
	// prepare the values
	bool first_render = true;
	bool invert = false;
	for (auto &collection : collections) {
		if (collection.Count() == 0) {
			continue;
		}
		if (first_render) {
			// add a separator if there are any rows
			render_rows.emplace_back(RenderRowType::SEPARATOR);
		} else {
			// render divider between top and bottom collection
			render_rows.emplace_back(RenderRowType::DIVIDER);
		}
		first_render = false;
		vector<BoxRenderRow> collection_rows;
		for (auto &chunk : collection.Chunks()) {
			vector<BoxRenderRow> chunk_rows;
			chunk_rows.resize(chunk.size());
			for (idx_t c = 0; c < column_count; c++) {
				auto string_data = FlatVector::GetData<string_t>(chunk.data[c]);
				for (idx_t r = 0; r < chunk.size(); r++) {
					string render_value;
					ResultRenderType render_type;
					ValueRenderAlignment alignment;
					LogicalType type;
					if (FlatVector::IsNull(chunk.data[c], r)) {
						render_value = config.null_value;
						render_type = ResultRenderType::NULL_VALUE;
					} else {
						render_value = ConvertRenderValue(string_data[r].GetString(), result_types[c]);
						render_type = ResultRenderType::VALUE;
					}
					if (config.render_mode == RenderMode::ROWS) {
						// in rows mode we select alignment for each column based on the type
						alignment = TypeAlignment(result_types[c]);
						type = result_types[c];
					} else {
						// in columns mode we left-align the header rows, and right-align the values
						switch (c) {
						case 0:
							render_type = ResultRenderType::COLUMN_NAME;
							alignment = ValueRenderAlignment::LEFT;
							break;
						case 1:
							render_type = ResultRenderType::COLUMN_TYPE;
							alignment = ValueRenderAlignment::LEFT;
							break;
						default:
							render_type = ResultRenderType::VALUE;
							alignment = ValueRenderAlignment::RIGHT;
							// for columns rendering mode - the type for this value is determined by the row index
							type = result.Types()[render_rows.size() + r - 2];
							break;
						}
					}
					chunk_rows[r].values.emplace_back(std::move(render_value), render_type, alignment, std::move(type));
				}
			}
			for (auto &row : chunk_rows) {
				collection_rows.push_back(std::move(row));
			}
		}
		if (config.large_number_rendering == LargeNumberRendering::FOOTER) {
			// when rendering the large number footer we align to the middle
			for (auto &row : collection_rows) {
				for (auto &value : row.values) {
					value.alignment = ValueRenderAlignment::MIDDLE;
				}
			}
			// large number footers should be rendered as NULL values
			for (auto &row : collection_rows[1].values) {
				row.render_mode = ResultRenderType::NULL_VALUE;
			}
		}
		if (invert) {
			// the bottom collection is inverted - so flip the rows
			std::reverse(collection_rows.begin(), collection_rows.end());
		}
		for (auto &row : collection_rows) {
			render_rows.push_back(std::move(row));
		}
		invert = true;
	}

	// now all rows are prepared - figure out the max width of each of the columns
	column_widths.resize(column_count, 0);
	for (auto &row : render_rows) {
		if (row.row_type != RenderRowType::ROW_VALUES) {
			continue;
		}
		D_ASSERT(row.values.size() == column_count);
		for (idx_t c = 0; c < column_count; c++) {
			auto render_width = Utf8Proc::RenderWidth(row.values[c].text);
			if (render_width > column_widths[c]) {
				column_widths[c] = render_width;
			}
			row.values[c].render_width = render_width;
		}
	}

	// figure out the total length
	// we start off with a pipe (|)
	total_render_length = 1;
	for (idx_t c = 0; c < column_widths.size(); c++) {
		// each column has a space at the beginning, and a space plus a pipe (|) at the end
		// hence + 3
		total_render_length += column_widths[c] + 3;
	}
	if (total_render_length < min_width) {
		// if there are hidden rows we should always display that
		// stretch up the first column until we have space to show the row count
		column_widths[0] += min_width - total_render_length;
		total_render_length = min_width;
	}
	// now we need to constrain the length
	unordered_set<idx_t> pruned_columns;
	bool shortened_columns = false;
	if (total_render_length > max_width) {
		auto original_widths = column_widths;
		// before we remove columns, check if we can just reduce the size of columns
		vector<idx_t> max_shorten_amount;
		idx_t total_max_shorten_amount = 0;
		for (auto &w : column_widths) {
			if (w <= config.max_col_width) {
				max_shorten_amount.push_back(0);
				continue;
			}
			auto max_diff = w - config.max_col_width;
			max_shorten_amount.push_back(max_diff);
			total_max_shorten_amount += max_diff;
		}
		if (total_max_shorten_amount >= total_render_length - max_width) {
			// we can get below the max width by shortening
			// try to shorten everything by an equivalent percentage
			// i.e. if we have two long string columns, we would prefer them to both end up as the same size
			idx_t shorten_amount_required = total_render_length - max_width;
			double percentage_shorten_amount = double(shorten_amount_required) / double(total_max_shorten_amount);
			D_ASSERT(percentage_shorten_amount >= 0 && percentage_shorten_amount <= 1);
			vector<idx_t> actual_shorten_amounts;
			idx_t total_shorten_amount = 0;
			for (auto &shorten_amount : max_shorten_amount) {
				if (shorten_amount == 0) {
					actual_shorten_amounts.push_back(0);
					continue;
				}
				idx_t new_shorten_amount =
				    static_cast<idx_t>(percentage_shorten_amount * LossyNumericCast<double>(shorten_amount));
				actual_shorten_amounts.push_back(new_shorten_amount);
				total_shorten_amount += new_shorten_amount;
			}
			D_ASSERT(total_shorten_amount <= shorten_amount_required);
			if (total_shorten_amount < shorten_amount_required) {
				// because of floating point truncation we might not get exactly enough shortening
				// ensure we
				idx_t remaining_shorten_amount = shorten_amount_required - total_shorten_amount;
				for (idx_t c = 0; c < actual_shorten_amounts.size() && remaining_shorten_amount > 0; c++) {
					idx_t possible_extra_shortening = max_shorten_amount[c] - actual_shorten_amounts[c];
					if (possible_extra_shortening == 0) {
						continue;
					}
					idx_t extra_shortening = MinValue<idx_t>(remaining_shorten_amount, possible_extra_shortening);
					remaining_shorten_amount -= extra_shortening;
					actual_shorten_amounts[c] += extra_shortening;
				}
				D_ASSERT(remaining_shorten_amount == 0);
			}
			// now perform the shortening
			for (idx_t c = 0; c < actual_shorten_amounts.size(); c++) {
				if (actual_shorten_amounts[c] == 0) {
					continue;
				}
				D_ASSERT(actual_shorten_amounts[c] < column_widths[c]);
				column_widths[c] -= actual_shorten_amounts[c];
				total_render_length -= actual_shorten_amounts[c];
				shortened_columns = true;
			}
		} else {
			// we cannot get below the max width by shortening
			// set everything that is wider than the col width to the max col width
			// afterwards - we need to prune columns
			for (auto &w : column_widths) {
				if (w <= config.max_col_width) {
					continue;
				}
				total_render_length -= w - config.max_col_width;
				w = config.max_col_width;
				shortened_columns = true;
			}
			D_ASSERT(total_render_length > max_width);
		}

		if (total_render_length > max_width) {
			// the total length is still too large
			// we need to remove columns!
			// first, we add 6 characters to the total length
			// this is what we need to add the "..." in the middle
			total_render_length += 3 + config.DOTDOTDOT_LENGTH;
			// now select columns to prune
			// we select columns in zig-zag order starting from the middle
			// e.g. if we have 10 columns, we remove #5, then #4, then #6, then #3, then #7, etc
			int64_t offset = 0;
			while (total_render_length > max_width) {
				auto c = NumericCast<idx_t>(NumericCast<int64_t>(column_count) / 2 + offset);
				total_render_length -= column_widths[c] + 3;
				pruned_columns.insert(c);
				if (offset >= 0) {
					offset = -offset - 1;
				} else {
					offset = -offset;
				}
			}

			// if we have any space left after truncating columns we can try to increase the size of columns again
			idx_t space_left = max_width - total_render_length;
			for (idx_t c = 0; c < column_widths.size() && space_left > 0; c++) {
				if (pruned_columns.find(c) != pruned_columns.end()) {
					// only increase size of visible columns
					continue;
				}
				if (column_widths[c] >= original_widths[c]) {
					continue;
				}
				idx_t increase_amount = MinValue<idx_t>(space_left, original_widths[c] - column_widths[c]);
				column_widths[c] += increase_amount;
				space_left -= increase_amount;
				total_render_length += increase_amount;
			}
		}
	}

	// update the footer with the column counts
	UpdateColumnCountFooter(column_count, pruned_columns);

	bool added_split_column = false;
	vector<idx_t> new_widths;
	vector<optional_idx> column_map;
	for (idx_t c = 0; c < column_count; c++) {
		if (pruned_columns.find(c) == pruned_columns.end()) {
			column_map.push_back(c);
			new_widths.push_back(column_widths[c]);
		} else if (!added_split_column) {
			// "..."
			column_map.push_back(optional_idx());
			new_widths.push_back(config.DOTDOTDOT_LENGTH);
			added_split_column = true;
		}
	}
	column_widths = std::move(new_widths);
	column_count = column_widths.size();

	// update the values based on the columns that were pruned
	for (auto &row : render_rows) {
		if (row.row_type != RenderRowType::ROW_VALUES) {
			continue;
		}
		vector<BoxRenderValue> values;
		for (idx_t c = 0; c < column_map.size(); c++) {
			auto column_idx = column_map[c];
			if (!column_idx.IsValid()) {
				// insert the split column
				values.emplace_back(config.DOTDOTDOT, ResultRenderType::LAYOUT, ValueRenderAlignment::MIDDLE);
				values.back().render_width = 1;
			} else {
				values.push_back(std::move(row.values[column_idx.GetIndex()]));
			}
		}
		row.values = std::move(values);
	}
	// check if we shortened any columns that would be rendered and if we can expand them
	if (shortened_columns && config.render_mode == RenderMode::ROWS && render_rows.size() < config.max_rows) {
		// if we have shortened any columns - try to expand them
		// how many rows do we have left to expand before we hit the max row limit?
		idx_t rows_left = config.max_rows - render_rows.size();
		// for each row - figure out if we can "expand" the row
		for (idx_t r = 0; r < render_rows.size(); r++) {
			auto &row = render_rows[r];
			if (row.row_type != RenderRowType::ROW_VALUES) {
				continue;
			}
			bool need_extra_row = r + 1 != render_rows.size() && r != 1;
			idx_t min_rows = need_extra_row ? 2 : 1;
			if (rows_left < min_rows) {
				// no rows left to expand
				continue;
			}
			// check if this row has truncated columns
			vector<BoxRenderRow> extra_rows;
			for (idx_t c = 0; c < row.values.size(); c++) {
				if (CanPrettyPrint(row.values[c].type)) {
					idx_t max_rows = rows_left + extra_rows.size();
					if (need_extra_row) {
						max_rows--;
					}
					PrettyPrintValue(row.values[c], max_rows, column_widths[c]);
					if (CanHighlight(row.values[c].type)) {
						HighlightValue(row.values[c]);
					}
					// FIXME: hacky
					row.values[c].render_width = column_widths[c] + 1;
				}
				auto render_width = row.values[c].render_width.GetIndex();
				if (render_width <= column_widths[c]) {
					// not shortened - skip
					if (c == 0) {
						// if the first row is not stretched out, we don't need to add a separator
						need_extra_row = false;
					}
					continue;
				}
				// this value was shortened! try to stretch it out
				// first truncate what appears on the first row
				idx_t current_row = 0;
				idx_t current_pos = 0;
				idx_t current_render_width = 0;
				auto full_value = row.values[c].text;
				auto annotations = row.values[c].annotations;
				idx_t annotation_idx = 0;
				ResultRenderType active_render_mode = ResultRenderType::VALUE;
				row.values[c].annotations.clear();
				row.values[c].text = TruncateValue(full_value, column_widths[c], current_pos, current_render_width);
				row.values[c].render_width = current_render_width;
				row.values[c].decomposed = true;
				// copy over annotations
				for (; annotation_idx < annotations.size(); annotation_idx++) {
					if (annotations[annotation_idx].start >= current_pos) {
						break;
					}
					row.values[c].annotations.push_back(annotations[annotation_idx]);
				}
				idx_t min_leftover_rows = need_extra_row ? 1 : 0;
				while (current_pos < full_value.size()) {
					if (current_row >= extra_rows.size()) {
						if (rows_left == min_leftover_rows) {
							// we need to add an extra row but there's no space anymore - break
							break;
						}
						// add a new row with empty values
						extra_rows.emplace_back();
						for (auto &current_val : row.values) {
							extra_rows.back().values.emplace_back(string(), current_val.render_mode,
							                                      current_val.alignment, current_val.type);
						}
						rows_left--;
					}
					bool can_add_extra_row = rows_left > min_leftover_rows;
					auto &extra_row = extra_rows[current_row++];
					idx_t start_pos = current_pos;
					// stretch out the remainder on this row
					current_render_width = 0;
					if (can_add_extra_row) {
						// if we can add an extra row after this row truncate it
						extra_row.values[c].text =
						    TruncateValue(full_value, column_widths[c], current_pos, current_render_width);
					} else {
						// if we cannot add an extra row after this just throw all remaining text on this row
						extra_row.values[c].text = full_value.substr(current_pos);
						current_render_width = Utf8Proc::RenderWidth(extra_row.values[c].text);
						current_pos = full_value.size();
					}
					extra_row.values[c].render_width = current_render_width;
					extra_row.values[c].decomposed = true;
					// copy over annotations
					if (active_render_mode != ResultRenderType::VALUE) {
						extra_row.values[c].annotations.emplace_back(active_render_mode, 0);
					}
					for (; annotation_idx < annotations.size(); annotation_idx++) {
						if (annotations[annotation_idx].start >= current_pos) {
							break;
						}
						annotations[annotation_idx].start -= start_pos;
						extra_row.values[c].annotations.push_back(annotations[annotation_idx]);
						active_render_mode = annotations[annotation_idx].render_mode;
					}
				}
			}
			if (extra_rows.empty()) {
				continue;
			}
			// if we added extra rows we need to add a separator if this is not the last row
			if (need_extra_row) {
				extra_rows.emplace_back(RenderRowType::SEPARATOR);
				rows_left--;
			}
			// add the extra rows at the current position
			render_rows.insert(render_rows.begin() + static_cast<int64_t>(r) + 1, extra_rows.begin(), extra_rows.end());
			r += extra_rows.size();
		}
	}
	// handle the row dividers
	for (idx_t r = 0; r < render_rows.size(); r++) {
		auto &row = render_rows[r];
		if (row.row_type != RenderRowType::DIVIDER) {
			continue;
		}
		// generate three new rows
		const idx_t divider_row_count = 3;
		vector<BoxRenderRow> divider_rows;
		for (idx_t d = 0; d < divider_row_count; d++) {
			divider_rows.emplace_back(RenderRowType::ROW_VALUES);
		}

		// find the prev/next rows
		idx_t prev_row_idx, next_row_idx;
		for (prev_row_idx = r; r > 0; r--) {
			if (render_rows[prev_row_idx - 1].row_type == RenderRowType::ROW_VALUES) {
				break;
			}
		}
		for (next_row_idx = r + 1; r < render_rows.size(); r++) {
			if (render_rows[next_row_idx].row_type == RenderRowType::ROW_VALUES) {
				break;
			}
		}
		if (prev_row_idx == 0 || next_row_idx >= render_rows.size()) {
			throw InternalException("No prev/next row found");
		}
		prev_row_idx--;
		auto &prev_row = render_rows[prev_row_idx];
		auto &next_row = render_rows[next_row_idx];
		// now generate the dividers for each of the columns

		for (idx_t c = 0; c < column_count; c++) {
			string str;
			auto &prev_value = prev_row.values[c];
			auto &next_value = next_row.values[c];
			ValueRenderAlignment alignment = prev_value.alignment;
			if (alignment == ValueRenderAlignment::MIDDLE) {
				// for middle alignment we don't have to do anything - just push a dot
				str = config.DOT;
			} else {
				// for left / right alignment we want to be in the middle of the prev / next value
				auto top_length = MinValue<idx_t>(column_widths[c], Utf8Proc::RenderWidth(prev_value.text));
				auto bottom_length = MinValue<idx_t>(column_widths[c], Utf8Proc::RenderWidth(next_value.text));
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
			for (idx_t d = 0; d < divider_row_count; d++) {
				divider_rows[d].values.emplace_back(str, ResultRenderType::LAYOUT, alignment);
			}
		}
		// override the divider row with the row values
		render_rows[r] = std::move(divider_rows[0]);
		// insert the extra divider rows
		for (idx_t d = 1; d < divider_row_count; d++) {
			render_rows.insert(render_rows.begin() + static_cast<int64_t>(r), std::move(divider_rows[d]));
		}
	}
}

void BoxRendererImplementation::RenderLayoutLine(const char *layout, const char *boundary, const char *left_corner,
                                                 const char *right_corner) {
	// render the top line
	ss << left_corner;
	idx_t column_index = 0;
	for (idx_t k = 0; k < total_render_length - 2; k++) {
		if (column_index < column_boundary_positions.size() && k == column_boundary_positions[column_index]) {
			ss << boundary;
			column_index++;
		} else {
			ss << layout;
		}
	}
	ss << right_corner;
	ss << '\n';
}

void BoxRendererImplementation::RenderValues() {
	auto column_count = column_widths.size();
	// render the top line
	RenderLayoutLine(config.HORIZONTAL, config.TMIDDLE, config.LTCORNER, config.RTCORNER);

	for (idx_t r = 0; r < render_rows.size(); r++) {
		auto &row = render_rows[r];
		if (row.row_type == RenderRowType::SEPARATOR) {
			// render separator
			RenderLayoutLine(config.HORIZONTAL, config.MIDDLE, config.LMIDDLE, config.RMIDDLE);
			continue;
		}
		if (row.row_type == RenderRowType::DIVIDER) {
			throw InternalException("Dividers should have been handled before");
		}
		// render row values
		for (idx_t column_idx = 0; column_idx < column_count; column_idx++) {
			auto &render_value = row.values[column_idx];
			auto render_mode = render_value.render_mode;
			auto alignment = render_value.alignment;
			if (render_mode == ResultRenderType::NULL_VALUE || render_mode == ResultRenderType::VALUE) {
				ss.SetValueType(render_value.type);
				if (!render_value.decomposed && CanHighlight(render_value.type)) {
					HighlightValue(render_value);
				}
			}
			RenderValue(render_value.text, column_widths[column_idx], render_mode, render_value.annotations, alignment,
			            render_value.render_width);
		}
		ss << config.VERTICAL;
		ss << '\n';
	}
}

void BoxRendererImplementation::RenderFooter(idx_t row_count, idx_t column_count) {
	auto &row_count_str = footer.row_count_str;
	auto &column_count_str = footer.column_count_str;
	auto &readable_rows_str = footer.readable_rows_str;
	auto &shown_str = footer.shown_str;
	auto &has_hidden_columns = footer.has_hidden_columns;
	auto &has_hidden_rows = footer.has_hidden_rows;
	// check if we can merge the row_count_str, readable_rows_str and the shown_str
	auto minimum_length = row_count_str.size() + column_count_str.size() + 6;
	bool render_rows_and_columns = total_render_length >= minimum_length &&
	                               ((has_hidden_columns && row_count > 0) || (row_count >= 10 && column_count > 1));
	bool render_rows = total_render_length >= footer.render_length && (row_count == 0 || row_count >= 10);
	bool render_anything = true;
	if (!render_rows && !render_rows_and_columns) {
		render_anything = false;
	}
	// render the bottom of the result values, if there are any
	RenderLayoutLine(config.HORIZONTAL, config.DMIDDLE, render_anything ? config.LMIDDLE : config.LDCORNER,
	                 render_anything ? config.RMIDDLE : config.RDCORNER);
	if (!render_anything) {
		return;
	}
	idx_t padding = total_render_length - row_count_str.size() - 4;
	if (render_rows_and_columns) {
		padding -= column_count_str.size();
	}
	string extra_render_str;
	// do we have to space to render the minimum_row_length and the shown string on the same row?
	idx_t shown_size = readable_rows_str.size() + shown_str.size() + (readable_rows_str.empty() ? 3 : 5);
	if (has_hidden_rows && padding >= shown_size) {
		// we have space - render it here
		extra_render_str = " (";
		if (!readable_rows_str.empty()) {
			extra_render_str += readable_rows_str + ", ";
		}
		extra_render_str += shown_str;
		extra_render_str += ")";
		D_ASSERT(extra_render_str.size() == shown_size);
		padding -= shown_size;
		readable_rows_str = string();
		shown_str = string();
	}

	ss << config.VERTICAL;
	ss << " ";
	if (render_rows_and_columns) {
		ss.Render(ResultRenderType::FOOTER, row_count_str);
		if (!extra_render_str.empty()) {
			ss.Render(ResultRenderType::NULL_VALUE, extra_render_str);
		}
		ss << string(padding, ' ');
		ss.Render(ResultRenderType::FOOTER, column_count_str);
	} else if (render_rows) {
		idx_t lpadding = padding / 2;
		idx_t rpadding = padding - lpadding;
		ss << string(lpadding, ' ');
		ss.Render(ResultRenderType::FOOTER, row_count_str);
		if (!extra_render_str.empty()) {
			ss.Render(ResultRenderType::NULL_VALUE, extra_render_str);
		}
		ss << string(rpadding, ' ');
	}
	ss << " ";
	ss << config.VERTICAL;
	ss << '\n';
	if (!readable_rows_str.empty() || !shown_str.empty()) {
		// we still need to render the readable rows/shown strings
		// check if we can merge the two onto one row
		idx_t combined_shown_length = readable_rows_str.size() + shown_str.size() + 4;
		if (!readable_rows_str.empty() && !shown_str.empty() && combined_shown_length <= total_render_length) {
			// we can! merge them
			ss << config.VERTICAL;
			ss << " ";
			ss.Render(ResultRenderType::NULL_VALUE, readable_rows_str);
			ss << string(total_render_length - combined_shown_length, ' ');
			ss.Render(ResultRenderType::NULL_VALUE, shown_str);
			ss << " ";
			ss << config.VERTICAL;
			ss << '\n';
			readable_rows_str = string();
			shown_str = string();
		}
		ValueRenderAlignment alignment =
		    render_rows_and_columns ? ValueRenderAlignment::LEFT : ValueRenderAlignment::MIDDLE;
		vector<HighlightingAnnotation> annotations;
		if (!readable_rows_str.empty()) {
			RenderValue("(" + readable_rows_str + ")", total_render_length - 4, ResultRenderType::NULL_VALUE,
			            annotations, alignment);
			ss << config.VERTICAL;
			ss << '\n';
		}
		if (!shown_str.empty()) {
			RenderValue("(" + shown_str + ")", total_render_length - 4, ResultRenderType::NULL_VALUE, annotations,
			            alignment);
			ss << config.VERTICAL;
			ss << '\n';
		}
	}
	// render the bottom line
	RenderLayoutLine(config.HORIZONTAL, config.HORIZONTAL, config.LDCORNER, config.RDCORNER);
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

void BoxRenderer::Render(ClientContext &context, const vector<string> &names, const ColumnDataCollection &result,
                         BaseResultRenderer &ss) {
	BoxRendererImplementation implementation(config, context, names, result, ss);
	implementation.Render();
}

} // namespace duckdb
