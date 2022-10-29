#include "duckdb/common/box_renderer.hpp"
#include "duckdb/common/types/column_data_collection.hpp"
#include "duckdb/common/printer.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include <sstream>
#include <list>

namespace duckdb {

BoxRenderer::BoxRenderer(BoxRendererConfig config_p) : config(move(config_p)) {
}

string BoxRenderer::ToString(ClientContext &context, const vector<string> &names, const ColumnDataCollection &result) {
	std::stringstream ss;
	Render(context, names, result, ss);
	return ss.str();
}

void BoxRenderer::Print(ClientContext &context, const vector<string> &names, const ColumnDataCollection &result) {
	Printer::Print(ToString(context, names, result));
}

void BoxRenderer::RenderValue(std::ostream &ss, const string &value, idx_t column_width, ValueRenderAlignment alignment) {
	auto render_width = Utf8Proc::RenderWidth(value);

	const string *render_value = &value;
	string small_value;
	if (render_width > column_width) {
		// the string is too large to fit in this column!
		// the size of this column must have been reduced
		// figure out how much of this value we can render
		idx_t pos = 0;
		idx_t current_render_width = 3;
		while(pos < value.size()) {
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
		small_value = value.substr(0, pos) + "...";
		render_value = &small_value;
		render_width = current_render_width;
	}
	auto padding_count = (column_width - render_width) + 2;
	idx_t lpadding;
	idx_t rpadding;
	switch(alignment) {
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
	ss << *render_value;
	ss << string(rpadding, ' ');
}


string BoxRenderer::RenderType(const LogicalType &type) {
	switch(type.id()) {
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
	case LogicalTypeId::LIST: {
		auto child = RenderType(ListType::GetChildType(type));
		return child + "[]";
	}
	default:
		return StringUtil::Lower(type.ToString());
	}
}

ValueRenderAlignment BoxRenderer::TypeAlignment(const LogicalType &type) {
	switch(type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return ValueRenderAlignment::RIGHT;
	default:
		return ValueRenderAlignment::LEFT;
	}
}

void BoxRenderer::Render(ClientContext &context, const vector<string> &names, const ColumnDataCollection &result, std::ostream &ss) {
	if (result.ColumnCount() != names.size()) {
		throw InternalException("Error in BoxRenderer::Render - unaligned columns and names");
	}
	auto max_width = config.max_width;
	if (max_width == 0) {
		if (Printer::IsTerminal()) {
			max_width = Printer::TerminalWidth();
		} else {
			max_width = 120;
		}
	}
	// we do not support max widths under 80
	max_width = MaxValue<idx_t>(80, max_width);

	// figure out how many/which rows to render
	idx_t row_count = result.Count();
	idx_t column_count = names.size();
	idx_t rows_to_render = MinValue<idx_t>(row_count, config.max_rows);
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
	string shown_str;
	bool has_hidden_rows = top_rows < row_count;
	if (has_hidden_rows) {
		shown_str = "(" + to_string(top_rows + bottom_rows) + " shown)";
	}
	auto minimum_row_length = MaxValue<idx_t>(row_count_str.size(), shown_str.size()) + 4;

	vector<LogicalType> varchar_types;
	for(idx_t c = 0; c < names.size(); c++) {
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

	// fetch the top rows from the ColumnDataCollection
	idx_t chunk_idx = 0;
	idx_t row_idx = 0;
	while(row_idx < top_rows) {
		fetch_result.Reset();
		insert_result.Reset();
		// fetch the next chunk
		result.FetchChunk(chunk_idx, fetch_result);
		idx_t insert_count = MinValue<idx_t>(fetch_result.size(), top_rows - row_idx);

		// cast all columns to varchar
		for(idx_t c = 0; c < column_count; c++) {
			VectorOperations::Cast(context, fetch_result.data[c], insert_result.data[c], insert_count);
		}
		insert_result.SetCardinality(insert_count);

		// construct the render collection
		top_collection.Append(insert_result);

		chunk_idx++;
		row_idx += fetch_result.size();
	}

	// fetch the bottom rows from the ColumnDataCollection
	row_idx = 0;
	chunk_idx = result.ChunkCount() - 1;
	while(row_idx < bottom_rows) {
		fetch_result.Reset();
		insert_result.Reset();
		// fetch the next chunk
		result.FetchChunk(chunk_idx, fetch_result);
		idx_t insert_count = MinValue<idx_t>(fetch_result.size(), bottom_rows - row_idx);

		// invert the rows
		SelectionVector inverted_sel(insert_count);
		for(idx_t r = 0; r < insert_count; r++) {
			inverted_sel.set_index(r, fetch_result.size() - r - 1);
		}

		for(idx_t c = 0; c < column_count; c++) {
			Vector slice(fetch_result.data[c], inverted_sel, insert_count);
			VectorOperations::Cast(context, slice, insert_result.data[c], insert_count);
		}
		insert_result.SetCardinality(insert_count);
		// construct the render collection
		bottom_collection.Append(insert_result);

		chunk_idx--;
		row_idx += fetch_result.size();
	}

	auto &result_types = result.Types();

	// for each column, figure out the width
	// start off by figuring out the name of the header by looking at the column name and column type
	vector<idx_t> widths;
	widths.reserve(column_count);
	for(idx_t c = 0; c < column_count; c++) {
		auto name_width = Utf8Proc::RenderWidth(names[c]);
		auto type_width = Utf8Proc::RenderWidth(RenderType(result_types[c]));
		widths.push_back(MaxValue<idx_t>(name_width, type_width));
	}

	// now iterate over the data in the render collection and find out the true max width
	for(auto &collection : collections) {
		for(auto &chunk : collection.Chunks()) {
			for(idx_t c = 0; c < column_count; c++) {
				auto string_data = FlatVector::GetData<string_t>(chunk.data[c]);
				for(idx_t r = 0; r < chunk.size(); r++) {
					auto render_width = Utf8Proc::RenderWidth(string_data[r].GetString());
					widths[c] = MaxValue<idx_t>(render_width, widths[c]);
				}
			}
		}
	}

	// render boundaries for the individual columns
	vector<idx_t> boundaries;
	// figure out the total length

	// we start off with a pipe (|)
	idx_t total_length = 1;
	for(idx_t c = 0; c < widths.size(); c++) {
		// each column has a space at the beginning, and a space plus a pipe (|) at the end
		// hence + 3
		total_length += widths[c] + 3;
	}
	if (has_hidden_rows && total_length < minimum_row_length) {
		// if there are hidden rows we should always display that
		// stretch up the first column until we have space to show the row count
		widths[0] += minimum_row_length - total_length;
		total_length = minimum_row_length;
	}
	// now we need to constrain the length
	vector<idx_t> column_map;
	unordered_set<idx_t> pruned_columns;
	if (total_length > max_width) {
		// before we remove columns, check if we can just reduce the size of columns
		for(auto &w : widths) {
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
			total_length += 6;
			// now select columns to prune
			// we select columns in zig-zag order starting from the middle
			// e.g. if we have 10 columns, we remove #5, then #4, then #6, then #3, then #7, etc
			int64_t offset = 0;
			while (total_length > max_width) {
				idx_t c = column_count / 2 + offset;
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

	const idx_t SPLIT_COLUMN = NumericLimits<idx_t>::Maximum();
	bool added_split_column = false;
	vector<idx_t> new_widths;
	for(idx_t c = 0; c < column_count; c++) {
		if (pruned_columns.find(c) == pruned_columns.end()) {
			column_map.push_back(c);
			new_widths.push_back(widths[c]);
		} else {
			if (!added_split_column) {
				// "..."
				column_map.push_back(SPLIT_COLUMN);
				new_widths.push_back(3);
				added_split_column = true;
			}
		}
	}
	widths = move(new_widths);
	column_count = column_map.size();

	for(idx_t c = 0; c < widths.size(); c++) {
		idx_t render_boundary;
		if (c == 0) {
			render_boundary = widths[c] + 2;
		} else {
			render_boundary = boundaries[c - 1] + widths[c] + 3;
		}
		boundaries.push_back(render_boundary);
	}


	// now begin rendering
	// render the top line
	ss << config.LTCORNER;
	idx_t column_index = 0;
	for(idx_t k = 0; k < total_length - 2; k++) {
		if (column_index + 1 < column_count && k == boundaries[column_index]) {
			ss << config.TMIDDLE;
			column_index++;
		} else {
			ss << config.HORIZONTAL;
		}
	}
	ss << config.RTCORNER;
	ss << std::endl;

	// render the header names
	for(idx_t c = 0; c < column_count; c++) {
		auto column_idx = column_map[c];
		string name;
		if (column_idx == SPLIT_COLUMN) {
			name = "...";
		} else {
			name = names[column_idx];
		}
		RenderValue(ss, name, widths[c]);
	}
	ss << config.VERTICAL;
	ss << std::endl;

	// render the types
	for(idx_t c = 0; c < column_count; c++) {
		auto column_idx = column_map[c];
		auto type = column_idx == SPLIT_COLUMN ? "" : RenderType(result_types[column_idx]);
		RenderValue(ss, type, widths[c]);
	}
	ss << config.VERTICAL;
	ss << std::endl;

	// render the line under the header
	ss << config.LMIDDLE;
	column_index = 0;
	for(idx_t k = 0; k < total_length - 2; k++) {
		if (column_index + 1 < column_count && k == boundaries[column_index]) {
			ss << config.MIDDLE;
			column_index++;
		} else {
			ss << config.HORIZONTAL;
		}
	}
	ss << config.RMIDDLE;
	ss << std::endl;

	// render the top rows
	auto rows = top_collection.GetRows();
	for(idx_t r = 0; r < top_rows; r++) {
		for(idx_t c = 0; c < column_count; c++) {
			auto column_idx = column_map[c];
			string str;
			if (column_idx == SPLIT_COLUMN) {
				str = "...";
			} else {
				auto row = rows.GetValue(column_idx, r);
				str = StringValue::Get(row);
			}
			RenderValue(ss, str, widths[c], TypeAlignment(result_types[c]));
		}
		ss << config.VERTICAL;
		ss << std::endl;
	}

	if (bottom_rows > 0) {
		// render the bottom rows
		// first render the divider
		for(idx_t k = 0; k < 3; k++) {
			for(idx_t c = 0; c < column_count; c++) {
				RenderValue(ss, ".", widths[c], TypeAlignment(result_types[c]));
			}
			ss << config.VERTICAL;
			ss << std::endl;
		}
		// note that the bottom rows are in reverse order
		auto brows = bottom_collection.GetRows();
		for(idx_t r = 0; r < bottom_rows; r++) {
			for(idx_t c = 0; c < column_count; c++) {
				auto column_idx = column_map[c];
				string str;
				if (column_idx == SPLIT_COLUMN) {
					str = "...";
				} else {
					auto row = brows.GetValue(column_idx, bottom_rows - r - 1);
					str = StringValue::Get(row);
				}
				RenderValue(ss, str, widths[c], TypeAlignment(result_types[c]));
			}
			ss << config.VERTICAL;
			ss << std::endl;
		}
	}

	// render the row count and column count
	auto column_count_str = to_string(result.ColumnCount()) + " column";
	if (result.ColumnCount() > 1) {
		column_count_str += "s";
	}
	if (column_count < result.ColumnCount()) {
		column_count_str += " (" + to_string(column_count) + " shown)";
	}
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
	bool render_rows_and_columns = total_length >= minimum_length && (row_count > 1 && column_count > 1);
	bool render_rows = total_length >= minimum_row_length && row_count > 1;
	if (render_rows || render_rows_and_columns || display_shown_separately) {
		// render the bottom of the result values
		ss << config.LMIDDLE;
		column_index = 0;
		for(idx_t k = 0; k < total_length - 2; k++) {
			if (column_index + 1 < column_count && k == boundaries[column_index]) {
				ss << config.DMIDDLE;
				column_index++;
			} else {
				ss << config.HORIZONTAL;
			}
		}
		ss << config.RMIDDLE;
		ss << std::endl;

		if (render_rows_and_columns) {
			ss << config.VERTICAL;
			ss << " ";
			ss << row_count_str;
			ss << string(total_length - row_count_str.size() - column_count_str.size() - 4, ' ');
			ss << column_count_str;
			ss << " ";
			ss << config.VERTICAL;
			ss << std::endl;
		} else if (render_rows) {
			RenderValue(ss, row_count_str, total_length - 4);
			ss << config.VERTICAL;
			ss << std::endl;

			if (display_shown_separately) {
				RenderValue(ss, shown_str, total_length - 4);
				ss << config.VERTICAL;
			ss << std::endl;
			}
		}
	}

	// render the bottom line
	ss << config.LDCORNER;
	column_index = 0;
	for(idx_t k = 0; k < total_length - 2; k++) {
		ss << config.HORIZONTAL;
	}
	ss << config.RDCORNER;
	ss << std::endl;
}

}
