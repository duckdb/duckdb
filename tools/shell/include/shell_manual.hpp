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

//! A single overload of a function, already formatted into a printable signature.
struct ManualOverload {
	//! Function name; overloads are grouped into an entry per (name, schema, type).
	string function_name;
	//! Raw function type from duckdb_functions() ("scalar", "aggregate", "table", ...); overloads are
	//! grouped under a heading per type and numbered sequentially in that grouped order.
	string function_type;
	//! Qualified schema the entry lives in ("database.schema"), shown once above each signature group.
	string schema_path;
	//! e.g. "list_value(any ANY, ...) -> LIST"
	string signature;
	//! Overload description (may be empty)
	string description;
	//! Overload examples (may be empty)
	vector<string> examples;
};

//! Build the printable signature string "name(a INTEGER, b VARCHAR, ...) -> RETURN".
//! `parameters` and `parameter_types` are positionally aligned; missing names fall back to "colN".
//! `varargs` is the vararg type when the overload is variadic, empty otherwise.
//! When `name_color`/`type_color` are non-empty, parameter names and (parameter/return/vararg) types
//! are wrapped in those terminal codes and `color_off`; each type token is colored independently so
//! the signature can still be word-wrapped safely.
string BuildSignature(const string &name, const vector<string> &parameters, const vector<string> &parameter_types,
                      const string &varargs, const string &return_type, const string &name_color = string(),
                      const string &type_color = string(), const string &color_off = string());

//! Terminal styling for the manual page. Each `*_on` is the escape sequence that begins a style and
//! `*_off` the reset; leaving a pair empty disables that coloring (e.g. off an interactive console).
struct ManualStyle {
	//! reference markers, rules and the "n / total" counter
	string layout_on, layout_off;
	//! section headings and the banner entry name
	string heading_on, heading_off;
	//! the header schema path and entry-type label
	string path_on, path_off;
};

//! Render the manual page for `overloads`, wrapped to `content_width` columns. Overloads are split into
//! entries by (name, schema, type); each entry opens with a rule carrying its schema, name and type,
//! followed by its signatures and deduplicated Descriptions/Examples sections. When more than one entry
//! is shown, each rule embeds a "n / total" position counter. `style` colors the structural elements
//! and `highlighter`, if set, syntax-highlights the examples. `pattern` is the searched-for text.
//! Returns the page text.
string RenderManualPage(const vector<ManualOverload> &overloads, const string &pattern, idx_t content_width,
                        const ManualStyle &style = ManualStyle(),
                        const ManualHighlighter &highlighter = ManualHighlighter());

} // namespace duckdb_shell
