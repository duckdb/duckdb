#include "gha_annotation.hpp"
#include "test_config.hpp"

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <regex>
#include <sstream>

namespace duckdb {
namespace gha {

bool IsGitHubActions() {
	const char *ci = std::getenv("CI");
	return ci && std::strcmp(ci, "true") == 0;
}

std::string StripWorkspacePrefix(const std::string &path) {
	const char *workspace = std::getenv("GITHUB_WORKSPACE");
	if (!workspace) {
		return path;
	}
	std::string prefix(workspace);
	if (!prefix.empty() && prefix.back() != '/') {
		prefix += '/';
	}
	if (path.compare(0, prefix.length(), prefix) == 0) {
		return path.substr(prefix.length());
	}
	return path;
}

std::string StripAnsiCodes(const std::string &str) {
	// ANSI escape sequences: ESC [ ... m (where ... is digits and semicolons)
	static const std::regex ansi_regex("\x1b\\[[0-9;]*m");
	return std::regex_replace(str, ansi_regex, "");
}

// Encode the annotation message body - Otherwise it cannot contain newlines
std::string EncodeMessage(const std::string &msg) {
	std::ostringstream result;
	for (char c : msg) {
		switch (c) {
		case '%':
			result << "%25";
			break;
		case '\r':
			result << "%0D";
			break;
		case '\n':
			result << "%0A";
			break;
		default:
			result << c;
			break;
		}
	}
	return result.str();
}

// Encode the title parameter - Escapes newlines, but also commas so it's not interpreted as the next parameter
// after one.
std::string EncodeTitle(const std::string &msg) {
	std::ostringstream result;
	for (char c : msg) {
		switch (c) {
		case '%':
			result << "%25";
			break;
		case '\r':
			result << "%0D";
			break;
		case '\n':
			result << "%0A";
			break;
		case ',':
			result << "%2C";
			break;
		default:
			result << c;
			break;
		}
	}
	return result.str();
}

void EmitError(const std::string &file, int line, const std::string &title, const std::string &message) {
	if (!IsGitHubActions()) {
		return;
	}

	std::ostringstream annotation;
	annotation << "::error";

	// Add optional parameters
	bool has_params = false;
	if (!file.empty()) {
		annotation << " file=" << StripWorkspacePrefix(file);
		has_params = true;
	}
	if (line > 0) {
		annotation << (has_params ? "," : " ") << "line=" << line;
		has_params = true;
	}
	if (!title.empty()) {
		annotation << (has_params ? "," : " ") << "title=" << EncodeTitle(title);
	}

	annotation << "::" << EncodeMessage(message) << "\n";

	std::cerr << annotation.str();
}

// Thread-local state for annotation metadata and start position in FailureSummary
static thread_local struct {
	std::string file;
	int line = 0;
	std::string title;
	size_t start_position = 0;
	bool active = false;
} annotation_state;

void BeginAnnotation(const std::string &file, int line, const std::string &title) {
	EndAnnotation(); // Emit any previous annotation
	annotation_state.file = file;
	annotation_state.line = line;
	annotation_state.title = title;
	annotation_state.start_position = FailureSummary::GetFailureSummaryAlways().size();
	annotation_state.active = true;
}

void EndAnnotation() {
	if (!annotation_state.active) {
		return;
	}
	std::string full_summary = FailureSummary::GetFailureSummaryAlways();
	std::string message = full_summary.substr(annotation_state.start_position);
	message = StripAnsiCodes(message);
	EmitError(annotation_state.file, annotation_state.line, annotation_state.title, message);
	annotation_state.active = false;
}

} // namespace gha
} // namespace duckdb
