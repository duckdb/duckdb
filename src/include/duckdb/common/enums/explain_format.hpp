//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/explain_format.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

//! ExplainFormat identifies an EXPLAIN / profiler output format by name (e.g. "json", "text"). It is a thin wrapper
//! around a string so that new output formats can be added without extending an enum. The set of valid format names
//! is defined by the registry in common/tree_renderer.cpp; use ExplainFormat::FromString to resolve a user-provided
//! name against it.
struct ExplainFormat {
	//! Defaults to the contextual default format (equivalent to ExplainFormat::DEFAULT()).
	ExplainFormat() : format("default") {
	}
	explicit ExplainFormat(string format) : format(std::move(format)) {
	}

	//! Named formats.
	static ExplainFormat DEFAULT() {
		return ExplainFormat("default");
	}
	static ExplainFormat TEXT() {
		return ExplainFormat("text");
	}
	static ExplainFormat JSON() {
		return ExplainFormat("json");
	}
	static ExplainFormat HTML() {
		return ExplainFormat("html");
	}
	static ExplainFormat GRAPHVIZ() {
		return ExplainFormat("graphviz");
	}
	static ExplainFormat YAML() {
		return ExplainFormat("yaml");
	}
	static ExplainFormat MERMAID() {
		return ExplainFormat("mermaid");
	}

	//! Resolve a (case-insensitive) format name against the registry, returning its canonical form. Throws
	//! InvalidInputException listing the valid format names when the name is not recognized.
	static ExplainFormat FromString(const string &name);

	bool operator==(const ExplainFormat &other) const {
		return format == other.format;
	}
	bool operator!=(const ExplainFormat &other) const {
		return !(*this == other);
	}

	//! The canonical (lowercase) format name.
	const string &ToString() const {
		return format;
	}

	string format;
};

} // namespace duckdb
