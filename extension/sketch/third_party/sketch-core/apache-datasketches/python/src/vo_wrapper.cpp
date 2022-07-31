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

#include "var_opt_sketch.hpp"
#include "var_opt_union.hpp"

#include <pybind11/pybind11.h>
#include <pybind11/functional.h>
#include <sstream>

namespace py = pybind11;

namespace datasketches {
namespace python {

template<typename T>
py::list vo_sketch_get_samples(const var_opt_sketch<T>& sk) {
  py::list list;
  for (auto item : sk) {
    py::tuple t = py::make_tuple(item.first, item.second);
    list.append(t);
  }
  return list;
}

template<typename T>
py::dict vo_sketch_estimate_subset_sum(const var_opt_sketch<T>& sk, const std::function<bool(T)> func) {
  subset_summary summary = sk.estimate_subset_sum(func);
  py::dict d;
  d["estimate"] = summary.estimate;
  d["lower_bound"] = summary.lower_bound;
  d["upper_bound"] = summary.upper_bound;
  d["total_sketch_weight"] = summary.total_sketch_weight;
  return d;
}

template<typename T>
std::string vo_sketch_to_string(const var_opt_sketch<T>& sk, bool print_items) {
  if (print_items) {
    std::ostringstream ss;
    ss << sk.to_string();
    ss << "### VarOpt Sketch Items" << std::endl;
    int i = 0;
    for (auto item : sk) {
      // item.second is always a double
      // item.first is an arbitrary py::object, so get the value by
      // using internal str() method then casting to C++ std::string
      py::str item_pystr(item.first);
      std::string item_str = py::cast<std::string>(item_pystr);
      // item.second is guaranteed to be a double
      ss << i++ << ": " << item_str << "\twt = " << item.second << std::endl;
    }
    return ss.str();
  } else {
    return sk.to_string();
  }
}

}
}

namespace dspy = datasketches::python;

template<typename T>
void bind_vo_sketch(py::module &m, const char* name) {
  using namespace datasketches;

  py::class_<var_opt_sketch<T>>(m, name)
    .def(py::init<uint32_t>(), py::arg("k"))
    .def("__str__", &dspy::vo_sketch_to_string<T>, py::arg("print_items")=false,
         "Produces a string summary of the sketch")
    .def("to_string", &dspy::vo_sketch_to_string<T>, py::arg("print_items")=false,
         "Produces a string summary of the sketch")
    .def("update", (void (var_opt_sketch<T>::*)(const T&, double)) &var_opt_sketch<T>::update, py::arg("item"), py::arg("weight")=1.0,
         "Updates the sketch with the given value and weight")
    .def_property_readonly("k", &var_opt_sketch<T>::get_k,
         "Returns the sketch's maximum configured sample size")
    .def_property_readonly("n", &var_opt_sketch<T>::get_n,
         "Returns the total stream length")
    .def_property_readonly("num_samples", &var_opt_sketch<T>::get_num_samples,
         "Returns the number of samples currently in the sketch")
    .def("get_samples", &dspy::vo_sketch_get_samples<T>,
         "Retyrns the set of samples in the sketch")
    .def("is_empty", &var_opt_sketch<T>::is_empty,
         "Returns True if the sketch is empty, otherwise False")
    .def("estimate_subset_sum", &dspy::vo_sketch_estimate_subset_sum<T>,
         "Applies a provided predicate to the sketch and returns the estimated total weight matching the predicate, as well "
         "as upper and lower bounds on the estimate and the total weight processed by the sketch")
    // As of writing, not yet clear how to serialize arbitrary python objects,
    // especially in any sort of language-portable way
    //.def("get_serialized_size_bytes", &var_opt_sketch<T>::get_serialized_size_bytes)
    //.def("serialize", &dspy::vo_sketch_serialize<T>)
    //.def_static("deserialize", &dspy::vo_sketch_deserialize<T>)
    ;
}

template<typename T>
void bind_vo_union(py::module &m, const char* name) {
  using namespace datasketches;

  py::class_<var_opt_union<T>>(m, name)
    .def(py::init<uint32_t>(), py::arg("max_k"))
    .def("__str__", &var_opt_union<T>::to_string,
         "Produces a string summary of the sketch")
    .def("to_string", &var_opt_union<T>::to_string,
         "Produces a string summary of the sketch")
    .def("update", (void (var_opt_union<T>::*)(const var_opt_sketch<T>& sk)) &var_opt_union<T>::update, py::arg("sketch"),
         "Updates the union with the given sketch")
    .def("get_result", &var_opt_union<T>::get_result,
         "Returns a sketch corresponding to the union result")
    .def("reset", &var_opt_union<T>::reset,
         "Resets the union to the empty state")
    // As of writing, not yet clear how to serialize arbitrary python objects,
    // especially in any sort of language-portable way
    //.def("get_serialized_size_bytes", &var_opt_sketch<T>::get_serialized_size_bytes)
    //.def("serialize", &dspy::vo_union_serialize<T>)
    //.def_static("deserialize", &dspy::vo_union_deserialize<T>)
    ;
}

void init_vo(py::module &m) {
  bind_vo_sketch<py::object>(m, "var_opt_sketch");
  bind_vo_union<py::object>(m, "var_opt_union");
}
