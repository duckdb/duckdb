#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/mutex.hpp"
 
namespace duckdb {
 
 //! The Timestamp Manager is responsible for creating and managing
 //! timestamps
class TimestampManager {
 public:
   static transaction_t GetHLCTimestamp();
   static void SetHLCTimestamp(transaction_t ts);
 private:
  static const uint64_t billion = 1000000000L;
  static transaction_t timestamp;
  static mutex timestamp_lock;
 };
 
 } // namespace duckdb
