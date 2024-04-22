#include "duckdb/main/error_manager.hpp"
#include "duckdb/main/config.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/common/exception/list.hpp"

namespace duckdb {

struct DefaultError {
	ErrorType type;
	const char *error;
};

static const DefaultError internal_errors[] = {
    {ErrorType::UNSIGNED_EXTENSION,
     "Extension \"%s\" could not be loaded because its signature is either missing or invalid and unsigned extensions "
     "are disabled by configuration (allow_unsigned_extensions)"},
    {ErrorType::INVALIDATED_TRANSACTION, "Current transaction is aborted (please ROLLBACK)"},
    {ErrorType::INVALIDATED_DATABASE, "Failed: database has been invalidated because of a previous fatal error. The "
                                      "database must be restarted prior to being used again.\nOriginal error: \"%s\""},
    {ErrorType::INVALID, nullptr}};

string ErrorManager::FormatExceptionRecursive(ErrorType error_type, vector<ExceptionFormatValue> &values) {
	if (error_type >= ErrorType::ERROR_COUNT) {
		throw InternalException("Invalid error type passed to ErrorManager::FormatError");
	}
	auto entry = custom_errors.find(error_type);
	string error;
	if (entry == custom_errors.end()) {
		// error was not overwritten
		error = internal_errors[int(error_type)].error;
	} else {
		// error was overwritten
		error = entry->second;
	}
	return ExceptionFormatValue::Format(error, values);
}

InvalidInputException ErrorManager::InvalidUnicodeError(const string &input, const string &context) {
	UnicodeInvalidReason reason;
	size_t pos;
	auto unicode = Utf8Proc::Analyze(const_char_ptr_cast(input.c_str()), input.size(), &reason, &pos);
	if (unicode != UnicodeType::INVALID) {
		return InvalidInputException("Invalid unicode error thrown but no invalid unicode detected in " + context);
	}
	string base_message;
	switch (reason) {
	case UnicodeInvalidReason::BYTE_MISMATCH:
		base_message = "Invalid unicode (byte sequence mismatch)";
		break;
	case UnicodeInvalidReason::INVALID_UNICODE:
		base_message = "Invalid unicode";
		break;
	default:
		break;
	}
	return InvalidInputException(base_message + " detected in " + context);
}

FatalException ErrorManager::InvalidatedDatabase(ClientContext &context, const string &invalidated_msg) {
	return FatalException(ErrorManager::FormatException(context, ErrorType::INVALIDATED_DATABASE, invalidated_msg));
}

TransactionException ErrorManager::InvalidatedTransaction(ClientContext &context) {
	return TransactionException(ErrorManager::FormatException(context, ErrorType::INVALIDATED_TRANSACTION));
}

void ErrorManager::AddCustomError(ErrorType type, string new_error) {
	custom_errors.insert(make_pair(type, std::move(new_error)));
}

ErrorManager &ErrorManager::Get(ClientContext &context) {
	return *DBConfig::GetConfig(context).error_manager;
}

ErrorManager &ErrorManager::Get(DatabaseInstance &context) {
	return *DBConfig::GetConfig(context).error_manager;
}

} // namespace duckdb
