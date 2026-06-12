//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/profiler/profiler_print_format.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

//! ProfilerPrintFormat identifies an EXPLAIN / profiler output format by name (e.g. "json", "text").
struct ProfilerPrintFormat {
	ProfilerPrintFormat() : format("default") {
	}
	explicit ProfilerPrintFormat(string format) : format(std::move(format)) {
	}

	//! Named formats.
	static ProfilerPrintFormat Default() {
		return ProfilerPrintFormat("default");
	}
	static ProfilerPrintFormat Text() {
		return ProfilerPrintFormat("text");
	}
	static ProfilerPrintFormat JSON() {
		return ProfilerPrintFormat("json");
	}
	static ProfilerPrintFormat HTML() {
		return ProfilerPrintFormat("html");
	}
	static ProfilerPrintFormat Graphviz() {
		return ProfilerPrintFormat("graphviz");
	}
	static ProfilerPrintFormat YAML() {
		return ProfilerPrintFormat("yaml");
	}
	static ProfilerPrintFormat Mermaid() {
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
