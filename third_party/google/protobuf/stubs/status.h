// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
// https://developers.google.com/protocol-buffers/
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#ifndef GOOGLE_PROTOBUF_STUBS_STATUS_H_
#define GOOGLE_PROTOBUF_STUBS_STATUS_H_

#include <string>

#include <google/protobuf/stubs/stringpiece.h>

#include <google/protobuf/port_def.inc>
namespace duckdb {
namespace google {
namespace protobuf {
namespace util {
namespace status_internal {

// These values must match error codes defined in google/rpc/code.proto.
enum class StatusCode : int {
	kOk = 0,
	kCancelled = 1,
	kUnknown = 2,
	kInvalidArgument = 3,
	kDeadlineExceeded = 4,
	kNotFound = 5,
	kAlreadyExists = 6,
	kPermissionDenied = 7,
	kUnauthenticated = 16,
	kResourceExhausted = 8,
	kFailedPrecondition = 9,
	kAborted = 10,
	kOutOfRange = 11,
	kUnimplemented = 12,
	kInternal = 13,
	kUnavailable = 14,
	kDataLoss = 15,
};

class  Status {
public:
	// Creates a "successful" status.
	Status();

	// Create a status in the canonical error space with the specified
	// code, and error message.  If "code == 0", error_message is
	// ignored and a Status object identical to Status::kOk is
	// constructed.
	Status(StatusCode error_code, StringPiece error_message);
	Status(const Status &);
	Status &operator=(const Status &x);
	~Status() {
	}

	// Accessor
	bool ok() const {
		return error_code_ == StatusCode::kOk;
	}
	StatusCode code() const {
		return error_code_;
	}
	StringPiece message() const {
		return error_message_;
	}

	bool operator==(const Status &x) const;
	bool operator!=(const Status &x) const {
		return !operator==(x);
	}

	// Return a combination of the error code name and message.
	std::string ToString() const;

private:
	StatusCode error_code_;
	std::string error_message_;
};

// Returns an OK status, equivalent to a default constructed instance. Prefer
// usage of `OkStatus()` when constructing such an OK status.
 Status OkStatus();

// Prints a human-readable representation of 'x' to 'os'.
 std::ostream &operator<<(std::ostream &os, const Status &x);

// These convenience functions return `true` if a given status matches the
// `StatusCode` error code of its associated function.
 bool IsAborted(const Status &status);
 bool IsAlreadyExists(const Status &status);
 bool IsCancelled(const Status &status);
 bool IsDataLoss(const Status &status);
 bool IsDeadlineExceeded(const Status &status);
 bool IsFailedPrecondition(const Status &status);
 bool IsInternal(const Status &status);
 bool IsInvalidArgument(const Status &status);
 bool IsNotFound(const Status &status);
 bool IsOutOfRange(const Status &status);
 bool IsPermissionDenied(const Status &status);
 bool IsResourceExhausted(const Status &status);
 bool IsUnauthenticated(const Status &status);
 bool IsUnavailable(const Status &status);
 bool IsUnimplemented(const Status &status);
 bool IsUnknown(const Status &status);

// These convenience functions create an `Status` object with an error code as
// indicated by the associated function name, using the error message passed in
// `message`.
//
// These functions are intentionally named `*Error` rather than `*Status` to
// match the names from Abseil:
// https://github.com/abseil/abseil-cpp/blob/2e9532cc6c701a8323d0cffb468999ab804095ab/absl/status/status.h#L716
 Status AbortedError(StringPiece message);
 Status AlreadyExistsError(StringPiece message);
 Status CancelledError(StringPiece message);
 Status DataLossError(StringPiece message);
 Status DeadlineExceededError(StringPiece message);
 Status FailedPreconditionError(StringPiece message);
 Status InternalError(StringPiece message);
 Status InvalidArgumentError(StringPiece message);
 Status NotFoundError(StringPiece message);
 Status OutOfRangeError(StringPiece message);
 Status PermissionDeniedError(StringPiece message);
 Status ResourceExhaustedError(StringPiece message);
 Status UnauthenticatedError(StringPiece message);
 Status UnavailableError(StringPiece message);
 Status UnimplementedError(StringPiece message);
 Status UnknownError(StringPiece message);

} // namespace status_internal

using duckdb::google::protobuf::util::status_internal::Status;
using duckdb::google::protobuf::util::status_internal::StatusCode;

using duckdb::google::protobuf::util::status_internal::IsAborted;
using duckdb::google::protobuf::util::status_internal::IsAlreadyExists;
using duckdb::google::protobuf::util::status_internal::IsCancelled;
using duckdb::google::protobuf::util::status_internal::IsDataLoss;
using duckdb::google::protobuf::util::status_internal::IsDeadlineExceeded;
using duckdb::google::protobuf::util::status_internal::IsFailedPrecondition;
using duckdb::google::protobuf::util::status_internal::IsInternal;
using duckdb::google::protobuf::util::status_internal::IsInvalidArgument;
using duckdb::google::protobuf::util::status_internal::IsNotFound;
using duckdb::google::protobuf::util::status_internal::IsOutOfRange;
using duckdb::google::protobuf::util::status_internal::IsPermissionDenied;
using duckdb::google::protobuf::util::status_internal::IsResourceExhausted;
using duckdb::google::protobuf::util::status_internal::IsUnauthenticated;
using duckdb::google::protobuf::util::status_internal::IsUnavailable;
using duckdb::google::protobuf::util::status_internal::IsUnimplemented;
using duckdb::google::protobuf::util::status_internal::IsUnknown;

using duckdb::google::protobuf::util::status_internal::AbortedError;
using duckdb::google::protobuf::util::status_internal::AlreadyExistsError;
using duckdb::google::protobuf::util::status_internal::CancelledError;
using duckdb::google::protobuf::util::status_internal::DataLossError;
using duckdb::google::protobuf::util::status_internal::DeadlineExceededError;
using duckdb::google::protobuf::util::status_internal::FailedPreconditionError;
using duckdb::google::protobuf::util::status_internal::InternalError;
using duckdb::google::protobuf::util::status_internal::InvalidArgumentError;
using duckdb::google::protobuf::util::status_internal::NotFoundError;
using duckdb::google::protobuf::util::status_internal::OkStatus;
using duckdb::google::protobuf::util::status_internal::OutOfRangeError;
using duckdb::google::protobuf::util::status_internal::PermissionDeniedError;
using duckdb::google::protobuf::util::status_internal::ResourceExhaustedError;
using duckdb::google::protobuf::util::status_internal::UnauthenticatedError;
using duckdb::google::protobuf::util::status_internal::UnavailableError;
using duckdb::google::protobuf::util::status_internal::UnimplementedError;
using duckdb::google::protobuf::util::status_internal::UnknownError;

} // namespace util
} // namespace protobuf
} // namespace google
} //namespace duckdb
#include <google/protobuf/port_undef.inc>

#endif  // GOOGLE_PROTOBUF_STUBS_STATUS_H_
