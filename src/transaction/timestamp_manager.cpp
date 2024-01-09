#include <time.h> /* for clock_gettime */
 #include "duckdb/transaction/timestamp_manager.hpp"
#include <algorithm>
 
namespace duckdb {
  transaction_t TimestampManager::timestamp = 0;
  mutex TimestampManager::timestamp_lock;
#define PHYSICAL_TIME_MASK 0xffffffffffffff00
#define LOGICAL_COUNTER_MASK 0x00000000000000ff
#define PHYSICAL_TIME_ROUNDUP_BIT 0x0000000000000080
#define PHYSICAL_TIME_ROUNDUP 0x0000000000000100  
  transaction_t TimestampManager::GetHLCTimestamp() {
    struct timespec ts;
    lock_guard<mutex> lock(timestamp_lock);
    clock_gettime(CLOCK_MONOTONIC, &ts);
    uint64_t ns = (ts.tv_sec * billion) + ts.tv_nsec;
    uint64_t pt = ns & PHYSICAL_TIME_MASK;
    uint32_t rb = ns & PHYSICAL_TIME_ROUNDUP_BIT;
    if (rb) {
      pt += PHYSICAL_TIME_ROUNDUP;
    }
    uint64_t new_timestamp;
    uint64_t current_pt = timestamp & PHYSICAL_TIME_MASK;
    uint64_t current_logical_counter = timestamp & LOGICAL_COUNTER_MASK;
    if (current_pt == pt) {
      ++current_logical_counter;
      new_timestamp = pt | current_logical_counter;
    } else {
      current_logical_counter = 0;
      new_timestamp = std::max(pt,current_pt) | current_logical_counter;
    }

    timestamp = new_timestamp;
    return new_timestamp;
   }
     
  void TimestampManager::SetHLCTimestamp(transaction_t message_ts) {
    struct timespec ts;
    lock_guard<mutex> lock(timestamp_lock);
    clock_gettime(CLOCK_MONOTONIC, &ts);
    uint64_t ns = (ts.tv_sec * billion) + ts.tv_nsec;
    uint64_t pt = ns & PHYSICAL_TIME_MASK;
    uint32_t rb = ns & PHYSICAL_TIME_ROUNDUP_BIT;
    if (rb) {
      pt += PHYSICAL_TIME_ROUNDUP;
    }

    uint64_t new_timestamp;
    uint64_t current_pt = timestamp & PHYSICAL_TIME_MASK;
    uint64_t message_pt = message_ts & PHYSICAL_TIME_MASK;
    uint64_t message_logical_counter = message_ts & LOGICAL_COUNTER_MASK;
    uint64_t max_pt = std::max({pt, current_pt, message_pt});
    uint64_t current_logical_counter = timestamp & LOGICAL_COUNTER_MASK;
    if ((current_pt == pt) && (pt == message_pt)) {
      current_logical_counter =  std::max(current_logical_counter, message_logical_counter) + 1;
      new_timestamp = pt | current_logical_counter;
    } else if (max_pt == current_pt) {
      ++current_logical_counter;
    } else if (max_pt == message_pt) {
      current_logical_counter = message_logical_counter + 1;
    } else {
      current_logical_counter = 0;
    }
    new_timestamp = max_pt | current_logical_counter;
    timestamp = new_timestamp;
   }
 }
