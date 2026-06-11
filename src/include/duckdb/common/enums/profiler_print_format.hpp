//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/profiler_print_format.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

//! ProfilerPrintFormat identifies an EXPLAIN / profiler output format by name (e.g. "json", "text"). It is a thin
//! wrapper around a string so that new output formats can be added without extending an enum. The set of valid format
//! names is defined by the registry in common/tree_renderer.cpp; use ProfilerPrintFormat::FromString to resolve a
//! user-provided name against it.
struct ProfilerPrintFormat {
	//! Defaults to the contextual default format (equivalent to ProfilerPrintFormat::DEFAULT()).
	ProfilerPrintFormat() : format("default") {
	}
	explicit ProfilerPrintFormat(string format) : format(std::move(format)) {
	}

	//! Named formats.
	static ProfilerPrintFormat DEFAULT() {
		return ProfilerPrintFormat("default");
	}
	static ProfilerPrintFormat TEXT() {
		return ProfilerPrintFormat("text");
	}
	static ProfilerPrintFormat JSON() {
		return ProfilerPrintFormat("json");
	}
	static ProfilerPrintFormat HTML() {
		return ProfilerPrintFormat("html");
	}
	static ProfilerPrintFormat GRAPHVIZ() {
		return ProfilerPrintFormat("graphviz");
	}
	static ProfilerPrintFormat YAML() {
		return ProfilerPrintFormat("yaml");
	}
	static ProfilerPrintFormat MERMAID() {
		return ProfilerPrintFormat("mermaid");
	}

	//! Resolve a (case-insensitive) format name against the registry, returning its canonical form. Throws
	//! InvalidInputException listing the valid format names when the name is not recognized.
	static ProfilerPrintFormat FromString(const string &name);

	bool operator==(const ProfilerPrintFormat &other) const {
		return format == other.format;
	}
	bool operator!=(const ProfilerPrintFormat &other) const {
		return !(*this == other);
	}

	//! The canonical (lowercase) format name.
	const string &ToString() const {
		return format;
	}

	string format;
};

} // namespace duckdb
