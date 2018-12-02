//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// common/file_system.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include <functional>
#include <string>

namespace duckdb {
//! Check if a directory exists
bool DirectoryExists(const std::string &directory);
//! Create a directory if it does not exist
void CreateDirectory(const std::string &directory);
//! Recursively remove a directory and all files in it
void RemoveDirectory(const std::string &directory);
//! List files in a directory, invoking the callback method for each one
bool ListFiles(const std::string &directory,
               std::function<void(std::string)> callback);
//! Sets the current working directory
void SetWorkingDirectory(const std::string &directory);
//! Gets the current working directory
std::string GetWorkingDirectory();
//! Check if a file exists
bool FileExists(const std::string &filename);
//! Path separator for the current file system
std::string PathSeparator();
//! Join two paths together
std::string JoinPath(const std::string &a, const std::string &path);
//! Sync a file descriptor to disk
void FileSync(FILE *file);

} // namespace duckdb
