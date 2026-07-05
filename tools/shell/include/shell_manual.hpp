//===----------------------------------------------------------------------===//
//                         DuckDB
//
// shell_manual.hpp
//
// Renders a "manual page" box for a SQL function - numbered signatures plus
// deduplicated Description and Examples sections. Used by the `.manual` command.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/string.hpp"

#include <functional>

namespace duckdb_shell {

using duckdb::idx_t;
using duckdb::string;
using duckdb::vector;

//! Applies syntax highlighting to a plain-text fragment, returning it with embedded ANSI codes
//! (or unchanged when highlighting is disabled).
using ManualHighlighter = std::function<string(const string &)>;

//! A single overload of a function, already formatted into a printable signature.
struct ManualOverload {
	//! Raw function type from duckdb_functions() ("scalar", "aggregate", "table", ...); overloads are
	//! grouped under a heading per type and numbered sequentially in that grouped order.
	string function_type;
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

//! Render the full manual page for the given overloads, wrapped to `content_width` columns. Signatures
//! are grouped under a heading per function type and numbered sequentially; the Descriptions and
//! Examples sections reference those numbers and deduplicate shared content. Box-drawing characters
//! (of the nested example boxes) are wrapped in `layout_on` / `layout_off` terminal codes and section
//! headings in `heading_on` / `heading_off` (empty to disable coloring). `highlighter`, if set,
//! syntax-highlights the examples. Returns the page text.
string RenderManualPage(const vector<ManualOverload> &overloads, idx_t content_width,
                        const string &layout_on = string(), const string &layout_off = string(),
                        const string &heading_on = string(), const string &heading_off = string(),
                        const ManualHighlighter &highlighter = ManualHighlighter());

} // namespace duckdb_shell
