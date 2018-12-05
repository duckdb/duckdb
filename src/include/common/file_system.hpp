//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

#include <functional>

namespace duckdb {
//! Check if a directory exists
bool DirectoryExists(const string &directory);
//! Create a directory if it does not exist
void CreateDirectory(const string &directory);
//! Recursively remove a directory and all files in it
void RemoveDirectory(const string &directory);
//! List files in a directory, invoking the callback method for each one
bool ListFiles(const string &directory, std::function<void(string)> callback);
//! Sets the current working directory
void SetWorkingDirectory(const string &directory);
//! Gets the current working directory
string GetWorkingDirectory();
//! Check if a file exists
bool FileExists(const string &filename);
//! Path separator for the current file system
string PathSeparator();
//! Join two paths together
string JoinPath(const string &a, const string &path);
//! Sync a file descriptor to disk
void FileSync(FILE *file);

} // namespace duckdb
