// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef ARROW_UTIL_LOGGING_H
#define ARROW_UTIL_LOGGING_H

#ifdef GANDIVA_IR

// The LLVM IR code doesn't have an NDEBUG mode. And, it shouldn't include references to
// streams or stdc++. So, making the DCHECK calls void in that case.

#define ARROW_IGNORE_EXPR(expr) ((void)(expr))

#define DCHECK(condition) ARROW_IGNORE_EXPR(condition)
#define DCHECK_OK(status) ARROW_IGNORE_EXPR(status)
#define DCHECK_EQ(val1, val2) ARROW_IGNORE_EXPR(val1)
#define DCHECK_NE(val1, val2) ARROW_IGNORE_EXPR(val1)
#define DCHECK_LE(val1, val2) ARROW_IGNORE_EXPR(val1)
#define DCHECK_LT(val1, val2) ARROW_IGNORE_EXPR(val1)
#define DCHECK_GE(val1, val2) ARROW_IGNORE_EXPR(val1)
#define DCHECK_GT(val1, val2) ARROW_IGNORE_EXPR(val1)

#else  // !GANDIVA_IR

#include <iostream>
#include <memory>
#include <string>

#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace util {

enum class ArrowLogLevel : int {
  ARROW_DEBUG = -1,
  ARROW_INFO = 0,
  ARROW_WARNING = 1,
  ARROW_ERROR = 2,
  ARROW_FATAL = 3
};

#define ARROW_LOG_INTERNAL(level) ::arrow::util::ArrowLog(__FILE__, __LINE__, level)
#define ARROW_LOG(level) ARROW_LOG_INTERNAL(::arrow::util::ArrowLogLevel::ARROW_##level)

#define ARROW_IGNORE_EXPR(expr) ((void)(expr))

#define ARROW_CHECK(condition)                                                         \
  (condition) ? ARROW_IGNORE_EXPR(0)                                                   \
              : ::arrow::util::Voidify() &                                             \
                    ::arrow::util::ArrowLog(__FILE__, __LINE__,                        \
                                            ::arrow::util::ArrowLogLevel::ARROW_FATAL) \
                        << " Check failed: " #condition " "

// If 'to_call' returns a bad status, CHECK immediately with a logged message
// of 'msg' followed by the status.
#define ARROW_CHECK_OK_PREPEND(to_call, msg)                \
  do {                                                      \
    ::arrow::Status _s = (to_call);                         \
    ARROW_CHECK(_s.ok()) << (msg) << ": " << _s.ToString(); \
  } while (false)

// If the status is bad, CHECK immediately, appending the status to the
// logged message.
#define ARROW_CHECK_OK(s) ARROW_CHECK_OK_PREPEND(s, "Bad status")

#ifdef NDEBUG
#define ARROW_DFATAL ::arrow::util::ArrowLogLevel::ARROW_WARNING

#define DCHECK(condition)       \
  ARROW_IGNORE_EXPR(condition); \
  while (false) ::arrow::util::ArrowLogBase()
#define DCHECK_OK(status)    \
  ARROW_IGNORE_EXPR(status); \
  while (false) ::arrow::util::ArrowLogBase()
#define DCHECK_EQ(val1, val2) \
  ARROW_IGNORE_EXPR(val1);    \
  while (false) ::arrow::util::ArrowLogBase()
#define DCHECK_NE(val1, val2) \
  ARROW_IGNORE_EXPR(val1);    \
  while (false) ::arrow::util::ArrowLogBase()
#define DCHECK_LE(val1, val2) \
  ARROW_IGNORE_EXPR(val1);    \
  while (false) ::arrow::util::ArrowLogBase()
#define DCHECK_LT(val1, val2) \
  ARROW_IGNORE_EXPR(val1);    \
  while (false) ::arrow::util::ArrowLogBase()
#define DCHECK_GE(val1, val2) \
  ARROW_IGNORE_EXPR(val1);    \
  while (false) ::arrow::util::ArrowLogBase()
#define DCHECK_GT(val1, val2) \
  ARROW_IGNORE_EXPR(val1);    \
  while (false) ::arrow::util::ArrowLogBase()

#else
#define ARROW_DFATAL ::arrow::util::ArrowLogLevel::ARROW_FATAL

#define DCHECK(condition) ARROW_CHECK(condition)
#define DCHECK_OK(status) (ARROW_CHECK((status).ok()) << (status).message())
#define DCHECK_EQ(val1, val2) ARROW_CHECK((val1) == (val2))
#define DCHECK_NE(val1, val2) ARROW_CHECK((val1) != (val2))
#define DCHECK_LE(val1, val2) ARROW_CHECK((val1) <= (val2))
#define DCHECK_LT(val1, val2) ARROW_CHECK((val1) < (val2))
#define DCHECK_GE(val1, val2) ARROW_CHECK((val1) >= (val2))
#define DCHECK_GT(val1, val2) ARROW_CHECK((val1) > (val2))

#endif  // NDEBUG

// This code is adapted from
// https://github.com/ray-project/ray/blob/master/src/ray/util/logging.h.

// To make the logging lib plugable with other logging libs and make
// the implementation unawared by the user, ArrowLog is only a declaration
// which hide the implementation into logging.cc file.
// In logging.cc, we can choose different log libs using different macros.

// This is also a null log which does not output anything.
class ARROW_EXPORT ArrowLogBase {
 public:
  virtual ~ArrowLogBase() {}

  virtual bool IsEnabled() const { return false; }

  template <typename T>
  ArrowLogBase& operator<<(const T& t) {
    if (IsEnabled()) {
      Stream() << t;
    }
    return *this;
  }

 protected:
  virtual std::ostream& Stream() { return std::cerr; }
};

class ARROW_EXPORT ArrowLog : public ArrowLogBase {
 public:
  ArrowLog(const char* file_name, int line_number, ArrowLogLevel severity);

  virtual ~ArrowLog();

  /// Return whether or not current logging instance is enabled.
  ///
  /// \return True if logging is enabled and false otherwise.
  virtual bool IsEnabled() const;

  /// The init function of arrow log for a program which should be called only once.
  ///
  /// \param appName The app name which starts the log.
  /// \param severity_threshold Logging threshold for the program.
  /// \param logDir Logging output file name. If empty, the log won't output to file.
  static void StartArrowLog(const std::string& appName,
                            ArrowLogLevel severity_threshold = ArrowLogLevel::ARROW_INFO,
                            const std::string& logDir = "");

  /// The shutdown function of arrow log, it should be used with StartArrowLog as a pair.
  static void ShutDownArrowLog();

  /// Install the failure signal handler to output call stack when crash.
  /// If glog is not installed, this function won't do anything.
  static void InstallFailureSignalHandler();

  /// Return whether or not the log level is enabled in current setting.
  ///
  /// \param log_level The input log level to test.
  /// \return True if input log level is not lower than the threshold.
  static bool IsLevelEnabled(ArrowLogLevel log_level);

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(ArrowLog);

  // Hide the implementation of log provider by void *.
  // Otherwise, lib user may define the same macro to use the correct header file.
  void* logging_provider_;
  /// True if log messages should be logged and false if they should be ignored.
  bool is_enabled_;

  static ArrowLogLevel severity_threshold_;
  // In InitGoogleLogging, it simply keeps the pointer.
  // We need to make sure the app name passed to InitGoogleLogging exist.
  static std::unique_ptr<std::string> app_name_;

 protected:
  virtual std::ostream& Stream();
};

// This class make ARROW_CHECK compilation pass to change the << operator to void.
// This class is copied from glog.
class ARROW_EXPORT Voidify {
 public:
  Voidify() {}
  // This has to be an operator with a precedence lower than << but
  // higher than ?:
  void operator&(ArrowLogBase&) {}
};

}  // namespace util
}  // namespace arrow
#endif  // GANDIVA_IR

#endif  // ARROW_UTIL_LOGGING_H
