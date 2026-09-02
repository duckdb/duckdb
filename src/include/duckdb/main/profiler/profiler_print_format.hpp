//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/profiler/profiler_print_format.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

//! ProfilerPrintFormat identifies an EXPLAIN / profiler output format by name (e.g. "json", "text").
struct ProfilerPrintFormat {
	ProfilerPrintFormat() : format("default") {
	}
	explicit ProfilerPrintFormat(Identifier format) : format(std::move(format)) {
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

	bool operator==(const ProfilerPrintFormat &other) const {
		return format == other.format;
	}
	bool operator!=(const ProfilerPrintFormat &other) const {
		return !(*this == other);
	}

	//! The format name as an identifier, for renderer lookup.
	const Identifier &ToIdentifier() const {
		return format;
	}

	Identifier format;
};

} // namespace duckdb
