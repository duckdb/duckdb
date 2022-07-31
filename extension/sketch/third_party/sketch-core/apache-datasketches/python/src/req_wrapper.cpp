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

#include "req_sketch.hpp"

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>
#include <sstream>
#include <vector>
#include <stdexcept>

namespace py = pybind11;

namespace datasketches {

namespace python {

template<typename T>
req_sketch<T> req_sketch_deserialize(py::bytes sk_bytes) {
  std::string sk_str = sk_bytes; // implicit cast  
  return req_sketch<T>::deserialize(sk_str.c_str(), sk_str.length());
}

template<typename T>
py::object req_sketch_serialize(const req_sketch<T>& sk) {
  auto ser_result = sk.serialize();
  return py::bytes((char*)ser_result.data(), ser_result.size());
}

// maybe possible to disambiguate the static vs method rank error calls, but
// this is easier for now
template<typename T>
double req_sketch_generic_normalized_rank_error(uint16_t k, bool pmf) {
  return req_sketch<T>::get_normalized_rank_error(k, pmf);
}

template<typename T>
double req_sketch_get_rank(const req_sketch<T>& sk,
                           const T& item,
                           bool inclusive) {
  if (inclusive)
    return sk.template get_rank<true>(item);
  else
    return sk.template get_rank<false>(item);
}

template<typename T>
T req_sketch_get_quantile(const req_sketch<T>& sk,
                          double rank,
                          bool inclusive) {
  if (inclusive)
    return T(sk.template get_quantile<true>(rank));
  else
    return T(sk.template get_quantile<false>(rank));
}

template<typename T>
py::list req_sketch_get_quantiles(const req_sketch<T>& sk,
                                  std::vector<double>& fractions,
                                  bool inclusive) {
  size_t n_quantiles = fractions.size();
  auto result = inclusive
     ? sk.template get_quantiles<true>(&fractions[0], n_quantiles)
     : sk.template get_quantiles<false>(&fractions[0], n_quantiles);

  // returning as std::vector<> would copy values to a list anyway
  py::list list(n_quantiles);
  for (size_t i = 0; i < n_quantiles; ++i) {
      list[i] = result[i];
  }

  return list;
}

template<typename T>
py::list req_sketch_get_pmf(const req_sketch<T>& sk,
                            std::vector<T>& split_points,
                            bool inclusive) {
  size_t n_points = split_points.size();
  auto result = inclusive
     ? sk.template get_PMF<true>(&split_points[0], n_points)
     : sk.template get_PMF<false>(&split_points[0], n_points);

  py::list list(n_points + 1);
  for (size_t i = 0; i <= n_points; ++i) {
    list[i] = result[i];
  }

  return list;
}

template<typename T>
py::list req_sketch_get_cdf(const req_sketch<T>& sk,
                            std::vector<T>& split_points,
                            bool inclusive) {
  size_t n_points = split_points.size();
  auto result = inclusive
     ? sk.template get_CDF<true>(&split_points[0], n_points)
     : sk.template get_CDF<false>(&split_points[0], n_points);

  py::list list(n_points + 1);
  for (size_t i = 0; i <= n_points; ++i) {
    list[i] = result[i];
  }

  return list;
}

template<typename T>
void req_sketch_update(req_sketch<T>& sk, py::array_t<T, py::array::c_style | py::array::forcecast> items) {
  if (items.ndim() != 1) {
    throw std::invalid_argument("input data must have only one dimension. Found: "
          + std::to_string(items.ndim()));
  }
  
  auto data = items.template unchecked<1>();
  for (uint32_t i = 0; i < data.size(); ++i) {
    sk.update(data(i));
  }
}

}
}

namespace dspy = datasketches::python;

template<typename T>
void bind_req_sketch(py::module &m, const char* name) {
  using namespace datasketches;

  py::class_<req_sketch<T>>(m, name)
    .def(py::init<uint16_t, bool>(), py::arg("k")=12, py::arg("is_hra")=true)
    .def(py::init<const req_sketch<T>&>())
    .def("update", (void (req_sketch<T>::*)(const T&)) &req_sketch<T>::update, py::arg("item"),
         "Updates the sketch with the given value")
    .def("update", &dspy::req_sketch_update<T>, py::arg("array"),
         "Updates the sketch with the values in the given array")
    .def("merge", (void (req_sketch<T>::*)(const req_sketch<T>&)) &req_sketch<T>::merge, py::arg("sketch"),
         "Merges the provided sketch into the this one")
    .def("__str__", &req_sketch<T>::to_string, py::arg("print_levels")=false, py::arg("print_items")=false,
         "Produces a string summary of the sketch")
    .def("to_string", &req_sketch<T>::to_string, py::arg("print_levels")=false, py::arg("print_items")=false,
         "Produces a string summary of the sketch")
    .def("is_hra", &req_sketch<T>::is_HRA,
         "Returns True if the sketch is in High Rank Accuracy mode, otherwise False")
    .def("is_empty", &req_sketch<T>::is_empty,
         "Returns True if the sketch is empty, otherwise False")
    .def("get_k", &req_sketch<T>::get_k,
         "Returns the configured parameter k")
    .def("get_n", &req_sketch<T>::get_n,
         "Returns the length of the input stream")
    .def("get_num_retained", &req_sketch<T>::get_num_retained,
         "Returns the number of retained items (samples) in the sketch")
    .def("is_estimation_mode", &req_sketch<T>::is_estimation_mode,
         "Returns True if the sketch is in estimation mode, otherwise False")
    .def("get_min_value", &req_sketch<T>::get_min_value,
         "Returns the minimum value from the stream. If empty, req_floats_sketch returns nan; req_ints_sketch throws a RuntimeError")
    .def("get_max_value", &req_sketch<T>::get_max_value,
         "Returns the maximum value from the stream. If empty, req_floats_sketch returns nan; req_ints_sketch throws a RuntimeError")
    .def("get_quantile", &dspy::req_sketch_get_quantile<T>, py::arg("rank"), py::arg("inclusive")=false,
         "Returns an approximation to the value of the data item "
         "that would be preceded by the given fraction of a hypothetical sorted "
         "version of the input stream so far.\n"
         "Note that this method has a fairly large overhead (microseconds instead of nanoseconds) "
         "so it should not be called multiple times to get different quantiles from the same "
         "sketch. Instead use get_quantiles(), which pays the overhead only once.\n"
         "For req_floats_sketch: if the sketch is empty this returns nan. "
         "For req_ints_sketch: if the sketch is empty this throws a RuntimeError.")
    .def("get_quantiles", &dspy::req_sketch_get_quantiles<T>, py::arg("ranks"), py::arg("inclusive")=false,
         "This is a more efficient multiple-query version of get_quantile().\n"
         "This returns an array that could have been generated by using get_quantile() for each "
         "fractional rank separately, but would be very inefficient. "
         "This method incurs the internal set-up overhead once and obtains multiple quantile values in "
         "a single query. It is strongly recommend that this method be used instead of multiple calls "
         "to get_quantile().\n"
         "If the sketch is empty this returns an empty vector.")
    .def("get_rank", &dspy::req_sketch_get_rank<T>, py::arg("item"), py::arg("inclusive")=false,
         "Returns an approximation to the normalized (fractional) rank of the given value from 0 to 1, inclusive.\n"
         "The resulting approximation has a probabilistic guarantee that can be obtained from the "
         "get_normalized_rank_error(False) function.\n"
         "With the parameter inclusive=true the weight of the given item is included into the rank."
         "Otherwise the rank equals the sum of the weights of items less than the given item.\n"
         "If the sketch is empty this returns nan.")
    .def("get_pmf", &dspy::req_sketch_get_pmf<T>, py::arg("split_points"), py::arg("inclusive")=false,
         "Returns an approximation to the Probability Mass Function (PMF) of the input stream "
         "given a set of split points (values).\n"
         "The resulting approximations have a probabilistic guarantee that can be obtained from the "
         "get_normalized_rank_error(True) function.\n"
         "If the sketch is empty this returns an empty vector.\n"
         "split_points is an array of m unique, monotonically increasing float values "
         "that divide the real number line into m+1 consecutive disjoint intervals.\n"
         "If the parameter inclusive=false, the definition of an 'interval' is inclusive of the left split point (or minimum value) and "
         "exclusive of the right split point, with the exception that the last interval will include "
         "the maximum value.\n"
         "If the parameter inclusive=true, the definition of an 'interval' is exclusive of the left split point (or minimum value) and "
         "inclusive of the right split point.\n"
         "It is not necessary to include either the min or max values in these split points.")
    .def("get_cdf", &dspy::req_sketch_get_cdf<T>, py::arg("split_points"), py::arg("inclusive")=false,
         "Returns an approximation to the Cumulative Distribution Function (CDF), which is the "
         "cumulative analog of the PMF, of the input stream given a set of split points (values).\n"
         "The resulting approximations have a probabilistic guarantee that can be obtained from the "
         "get_normalized_rank_error(True) function.\n"
         "If the sketch is empty this returns an empty vector.\n"
         "split_points is an array of m unique, monotonically increasing float values "
         "that divide the real number line into m+1 consecutive disjoint intervals.\n"
         "If the parameter inclusive=false, the definition of an 'interval' is inclusive of the left split point (or minimum value) and "
         "exclusive of the right split point, with the exception that the last interval will include "
         "the maximum value.\n"
         "If the parameter inclusive=true, the definition of an 'interval' is exclusive of the left split point (or minimum value) and "
         "inclusive of the right split point.\n"
         "It is not necessary to include either the min or max values in these split points.")
    .def("get_rank_lower_bound", &req_sketch<T>::get_rank_lower_bound, py::arg("rank"), py::arg("num_std_dev"),
         "Returns an approximate lower bound on the given normalized rank.\n"
         "Normalized rank must be a value between 0.0 and 1.0 (inclusive); "
         "the number of standard deviations must be 1, 2, or 3.")
    .def("get_rank_upper_bound", &req_sketch<T>::get_rank_upper_bound, py::arg("rank"), py::arg("num_std_dev"),
         "Returns an approximate upper bound on the given normalized rank.\n"
         "Normalized rank must be a value between 0.0 and 1.0 (inclusive); "
         "the number of standard deviations must be 1, 2, or 3.")
    .def_static("get_RSE", &req_sketch<T>::get_RSE,
         py::arg("k"), py::arg("rank"), py::arg("is_hra"), py::arg("n"),
         "Returns an a priori estimate of relative standard error (RSE, expressed as a number in [0,1]). "
         "Derived from Lemma 12 in http://arxiv.org/abs/2004.01668v2, but the constant factors have been "
         "modified based on empirical measurements, for a given value of parameter k.\n"
         "Normalized rank must be a value between 0.0 and 1.0 (inclusive). If is_hra is True, uses high "
         "rank accuracy mode, else low rank accuracy. N is an estimate of the total number of points "
         "provided to the sketch.")
    .def("serialize", &dspy::req_sketch_serialize<T>, "Serializes the sketch into a bytes object")
    .def_static("deserialize", &dspy::req_sketch_deserialize<T>, "Deserializes the sketch from a bytes object")
    ;
}

void init_req(py::module &m) {
  bind_req_sketch<int>(m, "req_ints_sketch");
  bind_req_sketch<float>(m, "req_floats_sketch");
}
