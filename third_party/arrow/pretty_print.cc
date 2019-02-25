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

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <sstream>  // IWYU pragma: keep
#include <string>
#include <type_traits>
#include <vector>

#include "arrow/array.h"
#include "arrow/pretty_print.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/string.h"
#include "arrow/visitor_inline.h"

namespace arrow {

using internal::checked_cast;

class PrettyPrinter {
 public:
  PrettyPrinter(int indent, int indent_size, int window, bool skip_new_lines,
                std::ostream* sink)
      : indent_(indent),
        indent_size_(indent_size),
        window_(window),
        skip_new_lines_(skip_new_lines),
        sink_(sink) {}

  void Write(const char* data);
  void Write(const std::string& data);
  void WriteIndented(const char* data);
  void WriteIndented(const std::string& data);
  void Newline();
  void Indent();
  void OpenArray(const Array& array);
  void CloseArray(const Array& array);

  void Flush() { (*sink_) << std::flush; }

 protected:
  int indent_;
  int indent_size_;
  int window_;
  bool skip_new_lines_;
  std::ostream* sink_;
};

void PrettyPrinter::OpenArray(const Array& array) {
  Indent();
  (*sink_) << "[";
  if (array.length() > 0) {
    (*sink_) << "\n";
    indent_ += indent_size_;
  }
}

void PrettyPrinter::CloseArray(const Array& array) {
  if (array.length() > 0) {
    indent_ -= indent_size_;
    Indent();
  }
  (*sink_) << "]";
}

void PrettyPrinter::Write(const char* data) { (*sink_) << data; }
void PrettyPrinter::Write(const std::string& data) { (*sink_) << data; }

void PrettyPrinter::WriteIndented(const char* data) {
  Indent();
  Write(data);
}

void PrettyPrinter::WriteIndented(const std::string& data) {
  Indent();
  Write(data);
}

void PrettyPrinter::Newline() {
  if (skip_new_lines_) {
    return;
  }
  (*sink_) << "\n";
  Indent();
}

void PrettyPrinter::Indent() {
  for (int i = 0; i < indent_; ++i) {
    (*sink_) << " ";
  }
}

class ArrayPrinter : public PrettyPrinter {
 public:
  ArrayPrinter(const Array& array, int indent, int indent_size, int window,
               const std::string& null_rep, bool skip_new_lines, std::ostream* sink)
      : PrettyPrinter(indent, indent_size, window, skip_new_lines, sink),
        array_(array),
        null_rep_(null_rep) {}

  template <typename FormatFunction>
  void WriteValues(const Array& array, FormatFunction&& func) {
    bool skip_comma = true;
    for (int64_t i = 0; i < array.length(); ++i) {
      if (skip_comma) {
        skip_comma = false;
      } else {
        (*sink_) << ",\n";
      }
      Indent();
      if ((i >= window_) && (i < (array.length() - window_))) {
        (*sink_) << "...\n";
        i = array.length() - window_ - 1;
        skip_comma = true;
      } else if (array.IsNull(i)) {
        (*sink_) << null_rep_;
      } else {
        func(i);
      }
    }
    (*sink_) << "\n";
  }

  template <typename T>
  inline typename std::enable_if<IsInteger<T>::value, Status>::type WriteDataValues(
      const T& array) {
    const auto data = array.raw_values();
    WriteValues(array, [&](int64_t i) { (*sink_) << static_cast<int64_t>(data[i]); });
    return Status::OK();
  }

  template <typename T>
  inline typename std::enable_if<IsFloatingPoint<T>::value, Status>::type WriteDataValues(
      const T& array) {
    const auto data = array.raw_values();
    WriteValues(array, [&](int64_t i) { (*sink_) << data[i]; });
    return Status::OK();
  }

  // String (Utf8)
  template <typename T>
  inline typename std::enable_if<std::is_same<StringArray, T>::value, Status>::type
  WriteDataValues(const T& array) {
    WriteValues(array, [&](int64_t i) { (*sink_) << "\"" << array.GetView(i) << "\""; });
    return Status::OK();
  }

  // Binary
  template <typename T>
  inline typename std::enable_if<std::is_same<BinaryArray, T>::value, Status>::type
  WriteDataValues(const T& array) {
    WriteValues(array, [&](int64_t i) { (*sink_) << HexEncode(array.GetView(i)); });
    return Status::OK();
  }

  template <typename T>
  inline
      typename std::enable_if<std::is_same<FixedSizeBinaryArray, T>::value, Status>::type
      WriteDataValues(const T& array) {
    WriteValues(array, [&](int64_t i) { (*sink_) << HexEncode(array.GetView(i)); });
    return Status::OK();
  }

  template <typename T>
  inline typename std::enable_if<std::is_same<Decimal128Array, T>::value, Status>::type
  WriteDataValues(const T& array) {
    WriteValues(array, [&](int64_t i) { (*sink_) << array.FormatValue(i); });
    return Status::OK();
  }

  template <typename T>
  inline typename std::enable_if<std::is_base_of<BooleanArray, T>::value, Status>::type
  WriteDataValues(const T& array) {
    WriteValues(array, [&](int64_t i) { Write(array.Value(i) ? "true" : "false"); });
    return Status::OK();
  }

  template <typename T>
  inline typename std::enable_if<std::is_base_of<ListArray, T>::value, Status>::type
  WriteDataValues(const T& array) {
    bool skip_comma = true;
    for (int64_t i = 0; i < array.length(); ++i) {
      if (skip_comma) {
        skip_comma = false;
      } else {
        (*sink_) << ",\n";
      }
      if ((i >= window_) && (i < (array.length() - window_))) {
        Indent();
        (*sink_) << "...\n";
        i = array.length() - window_ - 1;
        skip_comma = true;
      } else if (array.IsNull(i)) {
        Indent();
        (*sink_) << null_rep_;
      } else {
        std::shared_ptr<Array> slice =
            array.values()->Slice(array.value_offset(i), array.value_length(i));
        RETURN_NOT_OK(PrettyPrint(*slice, {indent_, window_}, sink_));
      }
    }
    (*sink_) << "\n";
    return Status::OK();
  }

  Status Visit(const NullArray& array) {
    (*sink_) << array.length() << " nulls";
    return Status::OK();
  }

  template <typename T>
  typename std::enable_if<std::is_base_of<PrimitiveArray, T>::value ||
                              std::is_base_of<FixedSizeBinaryArray, T>::value ||
                              std::is_base_of<BinaryArray, T>::value ||
                              std::is_base_of<ListArray, T>::value,
                          Status>::type
  Visit(const T& array) {
    OpenArray(array);
    if (array.length() > 0) {
      RETURN_NOT_OK(WriteDataValues(array));
    }
    CloseArray(array);
    return Status::OK();
  }

  Status Visit(const IntervalArray&) { return Status::NotImplemented("interval"); }

  Status WriteValidityBitmap(const Array& array);

  Status PrintChildren(const std::vector<std::shared_ptr<Array>>& fields, int64_t offset,
                       int64_t length) {
    for (size_t i = 0; i < fields.size(); ++i) {
      Newline();
      std::stringstream ss;
      ss << "-- child " << i << " type: " << fields[i]->type()->ToString() << "\n";
      Write(ss.str());

      std::shared_ptr<Array> field = fields[i];
      if (offset != 0) {
        field = field->Slice(offset, length);
      }

      RETURN_NOT_OK(PrettyPrint(*field, indent_ + indent_size_, sink_));
    }
    return Status::OK();
  }

  Status Visit(const StructArray& array) {
    RETURN_NOT_OK(WriteValidityBitmap(array));
    std::vector<std::shared_ptr<Array>> children;
    children.reserve(array.num_fields());
    for (int i = 0; i < array.num_fields(); ++i) {
      children.emplace_back(array.field(i));
    }
    return PrintChildren(children, 0, array.length());
  }

  Status Visit(const UnionArray& array) {
    RETURN_NOT_OK(WriteValidityBitmap(array));

    Newline();
    Write("-- type_ids: ");
    UInt8Array type_ids(array.length(), array.type_ids(), nullptr, 0, array.offset());
    RETURN_NOT_OK(PrettyPrint(type_ids, indent_ + indent_size_, sink_));

    if (array.mode() == UnionMode::DENSE) {
      Newline();
      Write("-- value_offsets: ");
      Int32Array value_offsets(array.length(), array.value_offsets(), nullptr, 0,
                               array.offset());
      RETURN_NOT_OK(PrettyPrint(value_offsets, indent_ + indent_size_, sink_));
    }

    // Print the children without any offset, because the type ids are absolute
    std::vector<std::shared_ptr<Array>> children;
    children.reserve(array.num_fields());
    for (int i = 0; i < array.num_fields(); ++i) {
      children.emplace_back(array.child(i));
    }
    return PrintChildren(children, 0, array.length() + array.offset());
  }

  Status Visit(const DictionaryArray& array) {
    Newline();
    Write("-- dictionary:\n");
    RETURN_NOT_OK(PrettyPrint(*array.dictionary(), indent_ + indent_size_, sink_));

    Newline();
    Write("-- indices:\n");
    return PrettyPrint(*array.indices(), indent_ + indent_size_, sink_);
  }

  Status Print() {
    RETURN_NOT_OK(VisitArrayInline(array_, this));
    Flush();
    return Status::OK();
  }

 private:
  const Array& array_;
  std::string null_rep_;
};

Status ArrayPrinter::WriteValidityBitmap(const Array& array) {
  Indent();
  Write("-- is_valid:");

  if (array.null_count() > 0) {
    Newline();
    BooleanArray is_valid(array.length(), array.null_bitmap(), nullptr, 0,
                          array.offset());
    return PrettyPrint(is_valid, indent_ + indent_size_, sink_);
  } else {
    Write(" all not null");
    return Status::OK();
  }
}

Status PrettyPrint(const Array& arr, int indent, std::ostream* sink) {
  ArrayPrinter printer(arr, indent, 2, 10, "null", false, sink);
  return printer.Print();
}

Status PrettyPrint(const Array& arr, const PrettyPrintOptions& options,
                   std::ostream* sink) {
  ArrayPrinter printer(arr, options.indent, options.indent_size, options.window,
                       options.null_rep, options.skip_new_lines, sink);
  return printer.Print();
}

Status PrettyPrint(const Array& arr, const PrettyPrintOptions& options,
                   std::string* result) {
  std::ostringstream sink;
  RETURN_NOT_OK(PrettyPrint(arr, options, &sink));
  *result = sink.str();
  return Status::OK();
}

Status PrettyPrint(const ChunkedArray& chunked_arr, const PrettyPrintOptions& options,
                   std::ostream* sink) {
  int num_chunks = chunked_arr.num_chunks();
  int indent = options.indent;
  int window = options.window;

  for (int i = 0; i < indent; ++i) {
    (*sink) << " ";
  }
  (*sink) << "[\n";
  bool skip_comma = true;
  for (int i = 0; i < num_chunks; ++i) {
    if (skip_comma) {
      skip_comma = false;
    } else {
      (*sink) << ",\n";
    }
    if ((i >= window) && (i < (num_chunks - window))) {
      for (int i = 0; i < indent; ++i) {
        (*sink) << " ";
      }
      (*sink) << "...\n";
      i = num_chunks - window - 1;
      skip_comma = true;
    } else {
      ArrayPrinter printer(*chunked_arr.chunk(i), indent + options.indent_size,
                           options.indent_size, window, options.null_rep,
                           options.skip_new_lines, sink);
      RETURN_NOT_OK(printer.Print());
    }
  }
  (*sink) << "\n";

  for (int i = 0; i < indent; ++i) {
    (*sink) << " ";
  }
  (*sink) << "]";

  return Status::OK();
}

Status PrettyPrint(const Column& column, const PrettyPrintOptions& options,
                   std::ostream* sink) {
  for (int i = 0; i < options.indent; ++i) {
    (*sink) << " ";
  }
  (*sink) << column.field()->ToString() << "\n";

  return PrettyPrint(*column.data(), options, sink);
}

Status PrettyPrint(const ChunkedArray& chunked_arr, const PrettyPrintOptions& options,
                   std::string* result) {
  std::ostringstream sink;
  RETURN_NOT_OK(PrettyPrint(chunked_arr, options, &sink));
  *result = sink.str();
  return Status::OK();
}

Status PrettyPrint(const RecordBatch& batch, int indent, std::ostream* sink) {
  for (int i = 0; i < batch.num_columns(); ++i) {
    const std::string& name = batch.column_name(i);
    (*sink) << name << ": ";
    RETURN_NOT_OK(PrettyPrint(*batch.column(i), indent + 2, sink));
    (*sink) << "\n";
  }
  (*sink) << std::flush;
  return Status::OK();
}

Status PrettyPrint(const Table& table, const PrettyPrintOptions& options,
                   std::ostream* sink) {
  RETURN_NOT_OK(PrettyPrint(*table.schema(), options, sink));
  (*sink) << "\n";
  (*sink) << "----\n";

  PrettyPrintOptions column_options = options;
  column_options.indent += 2;
  for (int i = 0; i < table.num_columns(); ++i) {
    for (int j = 0; j < options.indent; ++j) {
      (*sink) << " ";
    }
    (*sink) << table.schema()->field(i)->name() << ":\n";
    RETURN_NOT_OK(PrettyPrint(*table.column(i)->data(), column_options, sink));
    (*sink) << "\n";
  }
  (*sink) << std::flush;
  return Status::OK();
}

Status DebugPrint(const Array& arr, int indent) {
  return PrettyPrint(arr, indent, &std::cout);
}

class SchemaPrinter : public PrettyPrinter {
 public:
  SchemaPrinter(const Schema& schema, int indent, int indent_size, int window,
                bool skip_new_lines, std::ostream* sink)
      : PrettyPrinter(indent, indent_size, window, skip_new_lines, sink),
        schema_(schema) {}

  Status PrintType(const DataType& type);
  Status PrintField(const Field& field);

  Status Print() {
    for (int i = 0; i < schema_.num_fields(); ++i) {
      if (i > 0) {
        Newline();
      }
      RETURN_NOT_OK(PrintField(*schema_.field(i)));
    }
    Flush();
    return Status::OK();
  }

 private:
  const Schema& schema_;
};

Status SchemaPrinter::PrintType(const DataType& type) {
  Write(type.ToString());
  if (type.id() == Type::DICTIONARY) {
    indent_ += indent_size_;
    Newline();
    Write("dictionary:\n");
    const auto& dict_type = checked_cast<const DictionaryType&>(type);
    RETURN_NOT_OK(PrettyPrint(*dict_type.dictionary(), indent_ + indent_size_, sink_));
    indent_ -= indent_size_;
  } else {
    for (int i = 0; i < type.num_children(); ++i) {
      Newline();

      std::stringstream ss;
      ss << "child " << i << ", ";

      indent_ += indent_size_;
      WriteIndented(ss.str());
      RETURN_NOT_OK(PrintField(*type.child(i)));
      indent_ -= indent_size_;
    }
  }
  return Status::OK();
}

Status SchemaPrinter::PrintField(const Field& field) {
  Write(field.name());
  Write(": ");
  return PrintType(*field.type());
}

Status PrettyPrint(const Schema& schema, const PrettyPrintOptions& options,
                   std::ostream* sink) {
  SchemaPrinter printer(schema, options.indent, options.indent_size, options.window,
                        options.skip_new_lines, sink);
  return printer.Print();
}

Status PrettyPrint(const Schema& schema, const PrettyPrintOptions& options,
                   std::string* result) {
  std::ostringstream sink;
  RETURN_NOT_OK(PrettyPrint(schema, options, &sink));
  *result = sink.str();
  return Status::OK();
}

}  // namespace arrow
