//===----------------------------------------------------------------------===//
//                         DuckDB
//
// gha_annotation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

namespace duckdb {
namespace gha {

//! Returns true if running in GitHub Actions (CI=true environment variable)
bool IsGitHubActions();

//! Strips the workspace prefix from a path so GitHub can map it to the repo root.
//! Uses GITHUB_WORKSPACE env var if available, otherwise falls back to known CI paths.
std::string StripWorkspacePrefix(const std::string &path);

//! Encodes a message for GitHub Actions workflow commands (newlines -> %0A, % -> %25)
std::string EncodeMessage(const std::string &msg);

//! Strips ANSI escape codes from a string (for cleaner annotation messages)
std::string StripAnsiCodes(const std::string &str);

//! Emits a GitHub Actions error annotation to stderr.
//! Format: ::error file=path,line=N,title=Title::Message
void EmitError(const std::string &file, int line, const std::string &title, const std::string &message);

//! Begin an annotation. Records current position in FailureSummary.
//! Call EndAnnotation() to emit content logged since this call.
void BeginAnnotation(const std::string &file, int line, const std::string &title);

//! Emit annotation with content logged since BeginAnnotation, then reset.
void EndAnnotation();

} // namespace gha
} // namespace duckdb
