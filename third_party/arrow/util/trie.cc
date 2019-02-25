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

#include "arrow/util/trie.h"

#include <iostream>
#include <utility>

#include "arrow/util/logging.h"

namespace arrow {
namespace internal {

Status Trie::Validate() const {
  const auto n_nodes = static_cast<fast_index_type>(nodes_.size());
  if (size_ > n_nodes) {
    return Status::Invalid("Number of entries larger than number of nodes");
  }
  for (const auto& node : nodes_) {
    if (node.found_index_ >= size_) {
      return Status::Invalid("Found index >= size");
    }
    if (node.child_lookup_ != -1 &&
        node.child_lookup_ * 256 >
            static_cast<fast_index_type>(lookup_table_.size() - 256)) {
      return Status::Invalid("Child lookup base doesn't point to 256 valid indices");
    }
  }
  for (const auto index : lookup_table_) {
    if (index >= n_nodes) {
      return Status::Invalid("Child lookup index out of bounds");
    }
  }
  return Status::OK();
}

void Trie::Dump(const Node* node, const std::string& indent) const {
  std::cerr << "[\"" << node->substring_ << "\"]";
  if (node->found_index_ >= 0) {
    std::cerr << " *";
  }
  std::cerr << "\n";
  if (node->child_lookup_ >= 0) {
    auto child_indent = indent + "   ";
    std::cerr << child_indent << "|\n";
    for (fast_index_type i = 0; i < 256; ++i) {
      auto child_index = lookup_table_[node->child_lookup_ * 256 + i];
      if (child_index >= 0) {
        const Node* child = &nodes_[child_index];
        std::cerr << child_indent << "|-> '" << static_cast<char>(i) << "' (" << i
                  << ") -> ";
        Dump(child, child_indent);
      }
    }
  }
}

void Trie::Dump() const { Dump(&nodes_[0], ""); }

TrieBuilder::TrieBuilder() { trie_.nodes_.push_back(Trie::Node{-1, -1, ""}); }

Status TrieBuilder::AppendChildNode(Trie::Node* parent, uint8_t ch, Trie::Node&& node) {
  if (parent->child_lookup_ == -1) {
    RETURN_NOT_OK(ExtendLookupTable(&parent->child_lookup_));
  }
  auto parent_lookup = parent->child_lookup_ * 256 + ch;

  DCHECK_EQ(trie_.lookup_table_[parent_lookup], -1);
  if (trie_.nodes_.size() >= static_cast<size_t>(kMaxIndex)) {
    return Status::CapacityError("Trie out of bounds");
  }
  trie_.nodes_.push_back(std::move(node));
  trie_.lookup_table_[parent_lookup] = static_cast<index_type>(trie_.nodes_.size() - 1);
  return Status::OK();
}

Status TrieBuilder::CreateChildNode(Trie::Node* parent, uint8_t ch,
                                    util::string_view substring) {
  const auto kMaxSubstringLength = Trie::kMaxSubstringLength;

  while (substring.length() > kMaxSubstringLength) {
    // Substring doesn't fit in node => create intermediate node
    auto mid_node = Trie::Node{-1, -1, substring.substr(0, kMaxSubstringLength)};
    RETURN_NOT_OK(AppendChildNode(parent, ch, std::move(mid_node)));
    // Recurse
    parent = &trie_.nodes_.back();
    ch = static_cast<uint8_t>(substring[kMaxSubstringLength]);
    substring = substring.substr(kMaxSubstringLength + 1);
  }

  // Create final matching node
  auto child_node = Trie::Node{trie_.size_, -1, substring};
  RETURN_NOT_OK(AppendChildNode(parent, ch, std::move(child_node)));
  ++trie_.size_;
  return Status::OK();
}

Status TrieBuilder::CreateChildNode(Trie::Node* parent, char ch,
                                    util::string_view substring) {
  return CreateChildNode(parent, static_cast<uint8_t>(ch), substring);
}

Status TrieBuilder::ExtendLookupTable(index_type* out_index) {
  auto cur_size = trie_.lookup_table_.size();
  auto cur_index = cur_size / 256;
  if (cur_index > static_cast<size_t>(kMaxIndex)) {
    return Status::CapacityError("Trie out of bounds");
  }
  trie_.lookup_table_.resize(cur_size + 256, -1);
  *out_index = static_cast<index_type>(cur_index);
  return Status::OK();
}

Status TrieBuilder::SplitNode(fast_index_type node_index, fast_index_type split_at) {
  Trie::Node* node = &trie_.nodes_[node_index];

  DCHECK_LT(split_at, node->substring_length());

  // Before:
  //   {node} -> [...]
  // After:
  //   {node} -> [c] -> {out_node} -> [...]
  auto child_node = Trie::Node{node->found_index_, node->child_lookup_,
                               node->substring_.substr(split_at + 1)};
  auto ch = node->substring_[split_at];
  node->child_lookup_ = -1;
  node->found_index_ = -1;
  node->substring_ = node->substring_.substr(0, split_at);
  RETURN_NOT_OK(AppendChildNode(node, ch, std::move(child_node)));

  return Status::OK();
}

Status TrieBuilder::Append(util::string_view s, bool allow_duplicate) {
  // Find or create node for string
  fast_index_type node_index = 0;
  fast_index_type pos = 0;
  fast_index_type remaining = static_cast<fast_index_type>(s.length());

  while (true) {
    Trie::Node* node = &trie_.nodes_[node_index];
    const auto substring_length = node->substring_length();
    const auto substring_data = node->substring_data();

    for (fast_index_type i = 0; i < substring_length; ++i) {
      if (remaining == 0) {
        // New string too short => need to split node
        RETURN_NOT_OK(SplitNode(node_index, i));
        // Current node matches exactly
        node = &trie_.nodes_[node_index];
        node->found_index_ = trie_.size_++;
        return Status::OK();
      }
      if (s[pos] != substring_data[i]) {
        // Mismatching substring => need to split node
        RETURN_NOT_OK(SplitNode(node_index, i));
        // Create new node for mismatching char
        node = &trie_.nodes_[node_index];
        return CreateChildNode(node, s[pos], s.substr(pos + 1));
      }
      ++pos;
      --remaining;
    }
    if (remaining == 0) {
      // Node matches exactly
      if (node->found_index_ >= 0) {
        if (allow_duplicate) {
          return Status::OK();
        } else {
          return Status::Invalid("Duplicate entry in trie");
        }
      }
      node->found_index_ = trie_.size_++;
      return Status::OK();
    }
    // Lookup child using next input character
    if (node->child_lookup_ == -1) {
      // Need to extend lookup table for this node
      RETURN_NOT_OK(ExtendLookupTable(&node->child_lookup_));
    }
    auto c = static_cast<uint8_t>(s[pos++]);
    --remaining;
    node_index = trie_.lookup_table_[node->child_lookup_ * 256 + c];
    if (node_index == -1) {
      // Child not found => need to create child node
      return CreateChildNode(node, c, s.substr(pos));
    }
    node = &trie_.nodes_[node_index];
  }
}

Trie TrieBuilder::Finish() { return std::move(trie_); }

}  // namespace internal
}  // namespace arrow
