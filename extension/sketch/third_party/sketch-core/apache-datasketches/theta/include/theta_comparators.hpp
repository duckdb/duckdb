/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef THETA_COMPARATORS_HPP_
#define THETA_COMPARATORS_HPP_

namespace datasketches {

template<typename ExtractKey>
struct compare_by_key {
  template<typename Entry1, typename Entry2>
  bool operator()(Entry1&& a, Entry2&& b) const {
    return ExtractKey()(std::forward<Entry1>(a)) < ExtractKey()(std::forward<Entry2>(b));
  }
};

// less than

template<typename Key, typename Entry, typename ExtractKey>
class key_less_than {
public:
  explicit key_less_than(const Key& key): key(key) {}
  bool operator()(const Entry& entry) const {
    return ExtractKey()(entry) < this->key;
  }
private:
  Key key;
};

} /* namespace datasketches */

#endif
