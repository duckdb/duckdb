#include "duckdb.hpp"
#include "shell_state.hpp"
#include "shell_highlight.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/showref.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/common/box_renderer.hpp"

namespace duckdb_shell {

bool ShellState::UseDescribeRenderMode(const duckdb::SQLStatement &statement, string &describe_table_name) {
	// identify if this is a DESCRIBE {table} or DESCRIBE {query} statement
	// if it is we use the special DESCRIBE render mode
	if (statement.type != duckdb::StatementType::SELECT_STATEMENT) {
		return false;
	}
	auto &select = statement.Cast<duckdb::SelectStatement>();
	if (select.node->type != duckdb::QueryNodeType::SELECT_NODE) {
		return false;
	}
	auto &select_node = select.node->Cast<duckdb::SelectNode>();
	if (select_node.from_table->type != duckdb::TableReferenceType::SHOW_REF) {
		return false;
	}
	auto &showref = select_node.from_table->Cast<duckdb::ShowRef>();
	if (showref.show_type == duckdb::ShowType::SUMMARY) {
		return false;
	}
	describe_table_name = "Describe";
	if (!showref.table_name.empty()) {
		describe_table_name = showref.table_name;
	} else if (showref.query && showref.query->type == duckdb::QueryNodeType::SELECT_NODE) {
		auto &show_select = showref.query->Cast<duckdb::SelectNode>();
		if (show_select.from_table->type == duckdb::TableReferenceType::BASE_TABLE) {
			auto &base_table = show_select.from_table->Cast<duckdb::BaseTableRef>();
			describe_table_name = base_table.table_name;
		}
	}
	return true;
}

struct ShellColumnRenderInfo {
	string column_name;
	string column_type;
	idx_t column_name_length;
	idx_t column_type_length;
	bool is_primary_key;
};

struct ColumnRenderRow {
	vector<ShellColumnRenderInfo> columns;
};

struct ShellTableRenderInfo {
	ShellTableRenderInfo(ShellTableInfo table, char decimal_sep);

	ShellTableInfo table;
	idx_t table_name_length;
	idx_t render_width;
	idx_t max_column_name_length;
	idx_t max_column_type_length;
	string estimated_size_text;
	optional_idx estimated_size_length;
	vector<ColumnRenderRow> column_renders;

	idx_t LineCount() const {
		idx_t constant_count = 4;
		if (estimated_size_length.IsValid()) {
			constant_count += 2;
		}
		return ColumnLines() + constant_count;
	}

	idx_t ColumnLines() const {
		return column_renders[0].columns.size();
	}

	void Truncate(idx_t max_render_width);
	void TruncateValueIfRequired(string &value, idx_t &render_length, idx_t max_render_width);
};

struct TableMetadataLine {
	idx_t render_height = 0;
	idx_t render_width = 0;
	idx_t max_column_name_length = 0;
	idx_t max_column_type_length = 0;
	vector<idx_t> tables;

	void RenderLine(ShellHighlight &highlight, const vector<ShellTableRenderInfo> &tables, idx_t line_idx,
	                bool last_line);
};

struct TableMetadataDisplayInfo {
	idx_t render_height = 0;
	idx_t render_width = 0;
	vector<TableMetadataLine> display_lines;
};

void TableMetadataLine::RenderLine(ShellHighlight &highlight, const vector<ShellTableRenderInfo> &table_list,
                                   idx_t line_idx, bool last_line) {
	// figure out the table to render
	idx_t table_idx = 0;
	for (; table_idx < tables.size(); table_idx++) {
		idx_t line_count = table_list[tables[table_idx]].LineCount();
		if (line_idx < line_count) {
			// line belongs to this table - render it
			break;
		}
		// line belongs to a subsequent table - subtract line count and move to next table
		line_idx -= line_count;
	}
	if (table_idx == tables.size()) {
		// we didn't render any table - break
		if (!last_line) {
			// we need to add spaces if this is not the last line
			highlight.PrintText(string(render_width, ' '), PrintOutput::STDOUT, HighlightElementType::LAYOUT);
		}
		return;
	}
	auto &render_table = table_list[tables[table_idx]];
	auto &table = render_table.table;
	auto layout_type = table.is_view ? HighlightElementType::VIEW_LAYOUT : HighlightElementType::TABLE_LAYOUT;
	duckdb::BoxRendererConfig config;
	if (line_idx == 0) {
		// first line - render layout
		string top_line;
		top_line = config.LTCORNER;
		for (idx_t i = 0; i < render_width - 2; i++) {
			top_line += config.HORIZONTAL;
		}
		top_line += config.RTCORNER;
		highlight.PrintText(top_line, PrintOutput::STDOUT, layout_type);
		return;
	}
	if (line_idx == 1) {
		// table name
		string table_name;
		idx_t space_count = render_width - render_table.table_name_length - 2;
		idx_t lspace = space_count / 2;
		idx_t rspace = space_count - lspace;
		string table_line = config.VERTICAL;
		table_line += string(lspace, ' ');
		highlight.PrintText(table_line, PrintOutput::STDOUT, layout_type);
		highlight.PrintText(table.table_name, PrintOutput::STDOUT, HighlightElementType::TABLE_NAME);
		table_line = string(rspace, ' ');
		table_line += config.VERTICAL;
		highlight.PrintText(table_line, PrintOutput::STDOUT, layout_type);
		return;
	}
	if (line_idx > 2 && line_idx < render_table.ColumnLines() + 3) {
		idx_t column_idx = line_idx - 3;
		// column line

		// if we have a long table name the table name might have stretched out the box
		// in that case we need to add padding here
		// compute the required padding
		idx_t total_render_width = render_width;
		idx_t per_column_render_width = max_column_name_length + max_column_type_length + 5;
		idx_t column_render_width = render_table.column_renders.size() * per_column_render_width;
		idx_t extra_render_width = total_render_width - column_render_width;
		idx_t render_width_per_column = extra_render_width / render_table.column_renders.size();

		for (idx_t render_idx = 0; render_idx < render_table.column_renders.size(); render_idx++) {
			auto &column_render = render_table.column_renders[render_idx];
			string column_line = render_idx == 0 ? config.VERTICAL : " ";
			bool is_last = render_idx + 1 == render_table.column_renders.size();
			if (column_idx < column_render.columns.size()) {
				auto &col = column_render.columns[column_idx];
				idx_t col_name_length = col.column_name_length;
				idx_t col_type_length = col.column_type_length;
				column_line += " ";
				highlight.PrintText(column_line, PrintOutput::STDOUT, layout_type);
				auto highlight_type =
				    col.is_primary_key ? HighlightElementType::PRIMARY_KEY_COLUMN : HighlightElementType::COLUMN_NAME;
				highlight.PrintText(col.column_name, PrintOutput::STDOUT, highlight_type);
				column_line = string(max_column_name_length - col_name_length + 1, ' ');
				if (extra_render_width > 0) {
					idx_t render_count = is_last ? extra_render_width : render_width_per_column;
					column_line += string(render_count, ' ');
					extra_render_width -= render_count;
				}
				highlight.PrintText(column_line, PrintOutput::STDOUT, layout_type);
				highlight.PrintText(col.column_type, PrintOutput::STDOUT, HighlightElementType::COLUMN_TYPE);
				column_line = string(max_column_type_length - col_type_length + 1, ' ');
			} else {
				// we have already rendered all columns for this line - pad with spaces
				column_line += string(max_column_name_length + max_column_type_length + 3, ' ');
			}
			column_line += is_last ? config.VERTICAL : " ";
			highlight.PrintText(column_line, PrintOutput::STDOUT, layout_type);
		}
		return;
	}
	if (line_idx == 2 || (table.estimated_size.IsValid() && line_idx == render_table.ColumnLines() + 3)) {
		// blank line after table name
		string blank_line = config.VERTICAL;
		blank_line += string(render_width - 2, ' ');
		blank_line += config.VERTICAL;
		highlight.PrintText(blank_line, PrintOutput::STDOUT, layout_type);
		return;
	}
	if (table.estimated_size.IsValid() && line_idx == render_table.ColumnLines() + 4) {
		idx_t space_count = render_width - render_table.estimated_size_length.GetIndex() - 2;
		idx_t lspace = space_count / 2;
		idx_t rspace = space_count - lspace;
		string estimated_size_line = config.VERTICAL;
		estimated_size_line += string(lspace, ' ');
		highlight.PrintText(estimated_size_line, PrintOutput::STDOUT, layout_type);
		highlight.PrintText(render_table.estimated_size_text, PrintOutput::STDOUT, HighlightElementType::COMMENT);
		estimated_size_line = string(rspace, ' ');
		estimated_size_line += config.VERTICAL;
		highlight.PrintText(estimated_size_line, PrintOutput::STDOUT, layout_type);
		return;
	}
	// bottom line
	string bottom_line;
	bottom_line = config.LDCORNER;
	for (idx_t i = 0; i < render_width - 2; i++) {
		bottom_line += config.HORIZONTAL;
	}
	bottom_line += config.RDCORNER;
	highlight.PrintText(bottom_line, PrintOutput::STDOUT, layout_type);
}

string FormatTableMetadataType(const string &type) {
	auto result = StringUtil::Lower(type);
	if (StringUtil::StartsWith(result, "decimal")) {
		return "decimal";
	}
	return result;
}

ShellTableRenderInfo::ShellTableRenderInfo(ShellTableInfo table_p, char decimal_sep) : table(std::move(table_p)) {
	table_name_length = ShellState::RenderLength(table.table_name);
	idx_t max_col_name_length = 0;
	idx_t max_col_type_length = 0;
	ColumnRenderRow render_row;
	for (auto &col_p : table.columns) {
		ShellColumnRenderInfo col_display;
		col_display.column_name = std::move(col_p.column_name);
		col_display.column_type = FormatTableMetadataType(col_p.column_type);
		col_display.column_name_length = ShellState::RenderLength(col_display.column_name);
		col_display.column_type_length = ShellState::RenderLength(col_display.column_type);
		col_display.is_primary_key = col_p.is_primary_key;
		max_col_name_length = duckdb::MaxValue<idx_t>(col_display.column_name_length, max_col_name_length);
		max_col_type_length = duckdb::MaxValue<idx_t>(col_display.column_type_length, max_col_type_length);
		render_row.columns.push_back(std::move(col_display));
	}
	table.columns.clear();
	column_renders.push_back(std::move(render_row));

	max_column_name_length = max_col_name_length;
	max_column_type_length = max_col_type_length;
	render_width = table_name_length;
	if (max_col_name_length + max_col_type_length + 1 > render_width) {
		render_width = max_col_name_length + max_col_type_length + 1;
	}
	if (table.estimated_size.IsValid()) {
		estimated_size_text = to_string(table.estimated_size.GetIndex());
		auto formatted = duckdb::BoxRenderer::TryFormatLargeNumber(estimated_size_text, decimal_sep);
		if (!formatted.empty()) {
			estimated_size_text = std::move(formatted);
		}
		estimated_size_text += " rows";
		estimated_size_length = ShellState::RenderLength(estimated_size_text);
		if (estimated_size_length.GetIndex() > render_width) {
			render_width = estimated_size_length.GetIndex();
		}
	}
	render_width = render_width + 4;
}

void ShellTableRenderInfo::TruncateValueIfRequired(string &value, idx_t &render_length, idx_t max_render_width) {
	if (render_length <= max_render_width) {
		return;
	}
	duckdb::BoxRendererConfig config;
	idx_t pos = 0;
	idx_t render_width = 0;
	value = duckdb::BoxRenderer::TruncateValue(value, max_render_width - config.DOTDOTDOT_LENGTH, pos, render_width) +
	        config.DOTDOTDOT;
	render_length = render_width + config.DOTDOTDOT_LENGTH;
}

void ShellTableRenderInfo::Truncate(idx_t max_render_width) {
	if (render_width <= max_render_width) {
		return;
	}

	// we exceeded the render width - we need to truncate
	TruncateValueIfRequired(table.table_name, table_name_length, max_render_width - 4);
	// figure out what we need to truncate
	idx_t total_column_length = max_column_name_length + max_column_type_length + 5;
	if (total_column_length > max_render_width) {
		// we need to truncate either column names or column types
		// prefer to keep the name as long as possible - only truncate it if it by itself almost exceeds the
		// width
		if (max_column_name_length + 10 > max_render_width) {
			max_column_name_length = max_render_width - 10;
		}
		total_column_length = max_column_name_length + max_column_type_length + 5;
		if (total_column_length > max_render_width) {
			max_column_type_length = max_render_width - max_column_name_length - 5;
		}
		// truncate all columns that we need to truncate
		idx_t max_name_length = max_column_name_length;
		idx_t max_type_length = max_column_type_length;
		for (auto &column_render : column_renders) {
			for (auto &col : column_render.columns) {
				// try to to truncate the name
				TruncateValueIfRequired(col.column_name, col.column_name_length, max_name_length);
				// try to to truncate the type
				TruncateValueIfRequired(col.column_type, col.column_type_length, max_type_length);
			}
		}
		render_width = max_render_width;
	}
}
void ShellState::RenderTableMetadata(vector<ShellTableInfo> &tables) {
	idx_t max_render_width = max_width == 0 ? duckdb::Printer::TerminalWidth() : max_width;
	if (max_render_width < 80) {
		max_render_width = 80;
	}
	duckdb::BoxRendererConfig config;
	// figure out the render width of each table
	vector<ShellTableRenderInfo> result;
	for (auto &table : tables) {
		result.emplace_back(table, config.decimal_separator);
	}
	for (auto &table : result) {
		// optionally truncate each table if we exceed the max render width
		table.Truncate(max_render_width);
	}
	// try to split up large tables
	for (auto &table : result) {
		static constexpr const idx_t SPLIT_THRESHOLD = 20;
		D_ASSERT(table.column_renders.size() == 1);
		if (table.column_renders[0].columns.size() <= SPLIT_THRESHOLD) {
			// not that many columns - keep it as-is
			continue;
		}
		// try to split up columns if possible
		idx_t max_split_count = (table.column_renders[0].columns.size() + SPLIT_THRESHOLD - 1) / SPLIT_THRESHOLD;
		idx_t width_per_split = table.max_column_name_length + table.max_column_type_length + 5;
		idx_t max_splits = max_render_width / width_per_split;
		D_ASSERT(max_split_count > 1);
		if (max_splits <= 1) {
			// too wide - cannot split
			continue;
		}
		idx_t split_count = duckdb::MinValue<idx_t>(max_split_count, max_splits);

		vector<ColumnRenderRow> new_renders;
		new_renders.resize(split_count);
		idx_t split_idx = 0;
		for (auto &col : table.column_renders[0].columns) {
			new_renders[split_idx % split_count].columns.push_back(std::move(col));
			split_idx++;
		}
		table.column_renders = std::move(new_renders);
		table.render_width = duckdb::MaxValue<idx_t>(table.render_width, split_count * width_per_split);
	}

	// sort from biggest to smallest table
	std::sort(result.begin(), result.end(), [](const ShellTableRenderInfo &a, const ShellTableRenderInfo &b) {
		return a.LineCount() > b.LineCount();
	});

	// try to colocate different tables on the same lines in a greedy manner
	vector<TableMetadataDisplayInfo> metadata_displays;
	duckdb::unordered_set<idx_t> displayed_tables;
	for (idx_t table_idx = 0; table_idx < result.size(); table_idx++) {
		if (displayed_tables.find(table_idx) != displayed_tables.end()) {
			// already displayed
			continue;
		}
		displayed_tables.insert(table_idx);
		auto &initial_table = result[table_idx];
		TableMetadataDisplayInfo metadata_display;
		// the first line always has only one table (this table)
		TableMetadataLine initial_line;
		initial_line.tables.push_back(table_idx);
		metadata_display.render_width = initial_line.render_width = initial_table.render_width;
		metadata_display.render_height = initial_line.render_height = initial_table.LineCount();
		initial_line.max_column_name_length = initial_table.max_column_name_length;
		initial_line.max_column_type_length = initial_table.max_column_type_length;
		metadata_display.display_lines.push_back(std::move(initial_line));

		// now for each table, check if we can co-locate it
		for (idx_t next_idx = table_idx + 1; next_idx < result.size(); next_idx++) {
			auto &current_table = result[next_idx];
			auto render_width = current_table.render_width;
			auto render_height = current_table.LineCount();
			// we have two choices with co-locating
			// we can EITHER add a new line
			// OR add it to an existing line
			// we prefer to add it to an existing line if possible
			if (render_height > metadata_display.render_height) {
				// if this table is bigger than the current render height we can never add it - so just skip it
				continue;
			}
			bool added = false;
			for (auto &existing_line : metadata_display.display_lines) {
				if (existing_line.render_height + render_height > metadata_display.render_height) {
					// does not fit!
					continue;
				}
				// adding the table here works out height wise - we could potentially add it here
				// however, we do need to "stretch" the line to fit the current unit
				idx_t max_col_name_width = existing_line.max_column_name_length;
				idx_t max_col_type_width = existing_line.max_column_type_length;
				if (current_table.max_column_name_length > max_col_name_width) {
					max_col_name_width = current_table.max_column_name_length;
				}
				if (current_table.max_column_type_length > max_col_type_width) {
					max_col_type_width = current_table.max_column_type_length;
				}
				idx_t new_rendering_width = duckdb::MaxValue<idx_t>(render_width, existing_line.render_width);
				new_rendering_width =
				    duckdb::MaxValue<idx_t>(new_rendering_width, max_col_name_width + max_col_type_width + 5);

				D_ASSERT(new_rendering_width >= existing_line.render_width);
				idx_t extra_width = new_rendering_width - existing_line.render_width;

				if (metadata_display.render_width + extra_width > max_render_width) {
					// the extra width makes us exceed the rendering width limit - we cannot add it here
					continue;
				}
				// we can add it here! extend the line and add the table
				existing_line.max_column_name_length = max_col_name_width;
				existing_line.max_column_type_length = max_col_type_width;
				existing_line.render_width += extra_width;
				existing_line.render_height += render_height;
				existing_line.tables.push_back(next_idx);
				added = true;
				break;
			}
			if (!added) {
				// if we couldn't add it to an existing line we might still be able to add a new line
				// but only if that fits width wise
				if (metadata_display.render_width + render_width <= max_render_width) {
					// it does! add an extra line
					TableMetadataLine new_line;
					new_line.tables.push_back(next_idx);
					new_line.render_width = render_width;
					new_line.render_height = render_height;
					new_line.max_column_name_length = current_table.max_column_name_length;
					new_line.max_column_type_length = current_table.max_column_type_length;
					metadata_display.render_width += render_width;
					metadata_display.display_lines.push_back(std::move(new_line));
					added = true;
				}
			}
			if (added) {
				// we added this table for rendering - add to the displayed tables list
				displayed_tables.insert(next_idx);
			}
		}
		std::sort(metadata_display.display_lines.begin(), metadata_display.display_lines.end(),
		          [](TableMetadataLine &a, TableMetadataLine &b) { return a.render_height > b.render_height; });
		metadata_displays.push_back(std::move(metadata_display));
	}

	idx_t line_count = 0;
	for (auto &display_line : metadata_displays) {
		line_count += display_line.render_height;
	}
	unique_ptr<PagerState> pager_setup;
	if (ShouldUsePager(line_count)) {
		// we should use a pager
		pager_setup = SetupPager();
	}
	// render the metadata
	ShellHighlight highlight(*this);
	for (auto &metadata_display : metadata_displays) {
		for (idx_t line_idx = 0; line_idx < metadata_display.render_height; line_idx++) {
			// construct the line
			for (idx_t table_line_idx = 0; table_line_idx < metadata_display.display_lines.size(); table_line_idx++) {
				bool is_last = table_line_idx + 1 == metadata_display.display_lines.size();
				metadata_display.display_lines[table_line_idx].RenderLine(highlight, result, line_idx, is_last);
			}
			highlight.PrintText("\n", PrintOutput::STDOUT, HighlightElementType::LAYOUT);
		}
	}
}

} // namespace duckdb_shell
