#include "gha_annotation_listener.hpp"

#include "gha_annotation.hpp"

namespace duckdb {

GHAAnnotationListener::GHAAnnotationListener(Catch::ReporterConfig const &config)
    : Catch::TestEventListenerBase(config) {
}

bool GHAAnnotationListener::assertionEnded(Catch::AssertionStats const &stats) {
	if (stats.assertionResult.isOk()) {
		return true;
	}

	const auto &result = stats.assertionResult;
	auto src = result.getSourceInfo();

	std::string file = src.file ? src.file : "";
	int line = static_cast<int>(src.line);

	// Skip explicit failures from .test files - SQLLogicTestLogger already emits
	// detailed annotations for those, so the FAIL() call is redundant
	if (result.getResultType() == Catch::ResultWas::ExplicitFailure) {
		if (file.size() >= 5 && file.substr(file.size() - 5) == ".test") {
			return true;
		}
	}

	std::string title = ResultTypeToTitle(result.getResultType());
	title += " (" + Catch::getResultCapture().getCurrentTestName() + ")";

	std::string message = title;
	if (result.hasMessage()) {
		message = result.getMessage();
	} else if (result.hasExpression()) {
		message = result.getExpressionInMacro();
	}

	gha::EmitError(file, line, title, message);
	return true;
}

std::string GHAAnnotationListener::ResultTypeToTitle(Catch::ResultWas::OfType type) {
	switch (type) {
	case Catch::ResultWas::ExpressionFailed:
		return "Assertion failed";
	case Catch::ResultWas::ThrewException:
		return "Unexpected exception";
	case Catch::ResultWas::FatalErrorCondition:
		return "Fatal error";
	case Catch::ResultWas::DidntThrowException:
		return "Expected exception not thrown";
	case Catch::ResultWas::ExplicitFailure:
		return "Explicit failure";
	default:
		return "Test failure";
	}
}

} // namespace duckdb
