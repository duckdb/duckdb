//===----------------------------------------------------------------------===//
//                         DuckDB
//
// shell_manual.hpp
//
// Renders a "manual page" for a SQL function - a header with the schema/name/type, then numbered
// signatures and deduplicated Description and Examples sections. Used by the `.manual` command.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "shell_state.hpp"
#include <functional>

namespace duckdb_shell {

//! Applies syntax highlighting to a plain-text fragment, returning it with embedded ANSI codes
//! (or unchanged when highlighting is disabled).
using ManualHighlighter = std::function<string(const string &)>;

//! A single item of the manual page - one row extracted from duckdb_functions().
struct ManualItem {
	//! Function name; items are grouped into an entry per (name, schema, type).
	string function_name;
	//! Raw function type from duckdb_functions() ("scalar", "aggregate", "table", ...); items are
	//! grouped under a heading per type and numbered sequentially in that grouped order.
	string function_type;
	//! Qualified schema the entry lives in ("database.schema"), shown once above each signature group.
	string schema_path;
	//! Parameter names, positionally aligned with `parameter_types`; missing names fall back to "colN".
	vector<string> parameters;
	//! Parameter types (may be empty strings, e.g. for macros)
	vector<string> parameter_types;
	//! The vararg type when the item is variadic, empty otherwise
	string varargs;
	//! Return type (may be empty, e.g. for table functions)
	string return_type;
	//! Item description (may be empty)
	string description;
	//! Item examples (may be empty)
	vector<string> examples;
};

//! Terminal styling for the manual page. Each `*_on` is the escape sequence that begins a style and
//! `*_off` the reset; leaving a pair empty disables that coloring (e.g. off an interactive console).
struct ManualStyle {
	//! reference markers, rules and the "n / total" counter
	string layout_on, layout_off;
	//! section headings and the banner entry name
	string heading_on, heading_off;
	//! the header schema path and entry-type label
	string path_on, path_off;
	//! parameter names within a signature
	string param_on, param_off;
	//! parameter/vararg/return types within a signature
	string type_on, type_off;
};

//! Render the manual page for `items`, wrapped to `content_width` columns. Items are split into
//! entries by (name, schema, type); each entry opens with a rule carrying its schema, name and type,
//! followed by its signatures and deduplicated Descriptions/Examples sections. When more than one entry
//! is shown, each rule embeds a "n / total" position counter. `style` colors the structural elements
//! and `highlighter`, if set, syntax-highlights the examples. Returns the page text.
string RenderManualPage(const vector<ManualItem> &items, idx_t content_width, const ManualStyle &style = ManualStyle(),
                        const ManualHighlighter &highlighter = ManualHighlighter());

} // namespace duckdb_shell
