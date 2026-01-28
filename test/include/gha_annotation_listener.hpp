//===----------------------------------------------------------------------===//
//                         DuckDB
//
// gha_annotation_listener.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#define CATCH_CONFIG_EXTERNAL_INTERFACES

#include "catch.hpp"

namespace duckdb {

//! Catch2 listener that emits GitHub Actions annotations for assertion failures.
//! Annotations include file/line information so they appear inline in the PR Files view.
class GHAAnnotationListener : public Catch::TestEventListenerBase {
public:
	explicit GHAAnnotationListener(Catch::ReporterConfig const &config);

	bool assertionEnded(Catch::AssertionStats const &stats) override;

private:
	static std::string ResultTypeToTitle(Catch::ResultWas::OfType type);
};

} // namespace duckdb
