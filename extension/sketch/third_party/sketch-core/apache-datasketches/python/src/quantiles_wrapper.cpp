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

#include "quantiles_sketch.hpp"

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>
#include <vector>

namespace py = pybind11;

namespace datasketches {

namespace python {

template<typename T>
quantiles_sketch<T> quantiles_sketch_deserialize(py::bytes sk_bytes) {
  std::string sk_str = sk_bytes; // implicit cast  
  return quantiles_sketch<T>::deserialize(sk_str.c_str(), sk_str.length());
}

template<typename T>
py::object quantiles_sketch_serialize(const quantiles_sketch<T>& sk) {
  auto ser_result = sk.serialize();
  return py::bytes((char*)ser_result.data(), ser_result.size());
}

// maybe possible to disambiguate the static vs method rank error calls, but
// this is easier for now
template<typename T>
double quantiles_sketch_generic_normalized_rank_error(uint16_t k, bool pmf) {
  return quantiles_sketch<T>::get_normalized_rank_error(k, pmf);
}

template<typename T>
double quantiles_sketch_get_rank(const quantiles_sketch<T>& sk,
                                 const T& item,
                                 bool inclusive) {
  if (inclusive)
    return sk.template get_rank<true>(item);
  else
    return sk.template get_rank<false>(item);
}

template<typename T>
T quantiles_sketch_get_quantile(const quantiles_sketch<T>& sk,
                                double rank,
                                bool inclusive) {
  if (inclusive)
    return T(sk.template get_quantile<true>(rank));
  else
    return T(sk.template get_quantile<false>(rank));
}

template<typename T>
py::list quantiles_sketch_get_quantiles(const quantiles_sketch<T>& sk,
                                        std::vector<double>& fractions,
                                        bool inclusive) {
  size_t n_quantiles = fractions.size();
  auto result = inclusive
     ? sk.template get_quantiles<true>(&fractions[0], static_cast<uint32_t>(n_quantiles))
     : sk.template get_quantiles<false>(&fractions[0], static_cast<uint32_t>(n_quantiles));

  // returning as std::vector<> would copy values to a list anyway
  py::list list(n_quantiles);
  for (size_t i = 0; i < n_quantiles; ++i) {
      list[i] = result[i];
  }

  return list;
}

template<typename T>
py::list quantiles_sketch_get_pmf(const quantiles_sketch<T>& sk,
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
py::list quantiles_sketch_get_cdf(const quantiles_sketch<T>& sk,
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
void quantiles_sketch_update(quantiles_sketch<T>& sk, py::array_t<T, py::array::c_style | py::array::forcecast> items) {
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
void bind_quantiles_sketch(py::module &m, const char* name) {
  using namespace datasketches;

  py::class_<quantiles_sketch<T>>(m, name)
    .def(py::init<uint16_t>(), py::arg("k")=quantiles_constants::DEFAULT_K)
    .def(py::init<const quantiles_sketch<T>&>())
    .def("update", (void (quantiles_sketch<T>::*)(const T&)) &quantiles_sketch<T>::update, py::arg("item"),
         "Updates the sketch with the given value")
    .def("update", &dspy::quantiles_sketch_update<T>, py::arg("array"),
         "Updates the sketch with the values in the given array")
    .def("merge", (void (quantiles_sketch<T>::*)(const quantiles_sketch<T>&)) &quantiles_sketch<T>::merge, py::arg("sketch"),
         "Merges the provided sketch into the this one")
    .def("__str__", &quantiles_sketch<T>::to_string, py::arg("print_levels")=false, py::arg("print_items")=false,
         "Produces a string summary of the sketch")
    .def("to_string", &quantiles_sketch<T>::to_string, py::arg("print_levels")=false, py::arg("print_items")=false,
         "Produces a string summary of the sketch")
    .def("is_empty", &quantiles_sketch<T>::is_empty,
         "Returns True if the sketch is empty, otherwise False")
    .def("get_k", &quantiles_sketch<T>::get_k,
         "Returns the configured parameter k")
    .def("get_n", &quantiles_sketch<T>::get_n,
         "Returns the length of the input stream")
    .def("get_num_retained", &quantiles_sketch<T>::get_num_retained,
         "Returns the number of retained items (samples) in the sketch")
    .def("is_estimation_mode", &quantiles_sketch<T>::is_estimation_mode,
         "Returns True if the sketch is in estimation mode, otherwise False")
    .def("get_min_value", &quantiles_sketch<T>::get_min_value,
         "Returns the minimum value from the stream. If empty, quantiles_floats_sketch returns nan; quantiles_ints_sketch throws a RuntimeError")
    .def("get_max_value", &quantiles_sketch<T>::get_max_value,
         "Returns the maximum value from the stream. If empty, quantiles_floats_sketch returns nan; quantiles_ints_sketch throws a RuntimeError")
    .def("get_quantile", &dspy::quantiles_sketch_get_quantile<T>, py::arg("rank"), py::arg("inclusive")=false,
         "Returns an approximation to the value of the data item "
         "that would be preceded by the given fraction of a hypothetical sorted "
         "version of the input stream so far.\n"
         "Note that this method has a fairly large overhead (microseconds instead of nanoseconds) "
         "so it should not be called multiple times to get different quantiles from the same "
         "sketch. Instead use get_quantiles(), which pays the overhead only once.\n"
         "For quantiles_floats_sketch: if the sketch is empty this returns nan. "
         "For quantiles_ints_sketch: if the sketch is empty this throws a RuntimeError.")
    .def("get_quantiles", &dspy::quantiles_sketch_get_quantiles<T>, py::arg("ranks"), py::arg("inclusive")=false,
         "This is a more efficient multiple-query version of get_quantile().\n"
         "This returns an array that could have been generated by using get_quantile() for each "
         "fractional rank separately, but would be very inefficient. "
         "This method incurs the internal set-up overhead once and obtains multiple quantile values in "
         "a single query. It is strongly recommend that this method be used instead of multiple calls "
         "to get_quantile().\n"
         "If the sketch is empty this returns an empty vector.")
    .def("get_rank", &dspy::quantiles_sketch_get_rank<T>, py::arg("item"), py::arg("inclusive")=false,
         "Returns an approximation to the normalized (fractional) rank of the given value from 0 to 1, inclusive.\n"
         "The resulting approximation has a probabilistic guarantee that can be obtained from the "
         "get_normalized_rank_error(False) function.\n"
         "If the sketch is empty this returns nan.")
    .def("get_pmf", &dspy::quantiles_sketch_get_pmf<T>, py::arg("split_points"), py::arg("inclusive")=false,
         "Returns an approximation to the Probability Mass Function (PMF) of the input stream "
         "given a set of split points (values).\n"
         "The resulting approximations have a probabilistic guarantee that can be obtained from the "
         "get_normalized_rank_error(True) function.\n"
         "If the sketch is empty this returns an empty vector.\n"
         "split_points is an array of m unique, monotonically increasing float values "
         "that divide the real number line into m+1 consecutive disjoint intervals.\n"
         "The definition of an 'interval' is inclusive of the left split point (or minimum value) and "
         "exclusive of the right split point, with the exception that the last interval will include "
         "the maximum value.\n"
         "It is not necessary to include either the min or max values in these split points.")
    .def("get_cdf", &dspy::quantiles_sketch_get_cdf<T>, py::arg("split_points"), py::arg("inclusive")=false,
         "Returns an approximation to the Cumulative Distribution Function (CDF), which is the "
         "cumulative analog of the PMF, of the input stream given a set of split points (values).\n"
         "The resulting approximations have a probabilistic guarantee that can be obtained from the "
         "get_normalized_rank_error(True) function.\n"
         "If the sketch is empty this returns an empty vector.\n"
         "split_points is an array of m unique, monotonically increasing float values "
         "that divide the real number line into m+1 consecutive disjoint intervals.\n"
         "The definition of an 'interval' is inclusive of the left split point (or minimum value) and "
         "exclusive of the right split point, with the exception that the last interval will include "
         "the maximum value.\n"
         "It is not necessary to include either the min or max values in these split points.")
    .def("normalized_rank_error", (double (quantiles_sketch<T>::*)(bool) const) &quantiles_sketch<T>::get_normalized_rank_error,
         py::arg("as_pmf"),
         "Gets the normalized rank error for this sketch.\n"
         "If pmf is True, returns the 'double-sided' normalized rank error for the get_PMF() function.\n"
         "Otherwise, it is the 'single-sided' normalized rank error for all the other queries.\n"
         "Constants were derived as the best fit to 99 percentile empirically measured max error in thousands of trials")
    .def_static("get_normalized_rank_error", &dspy::quantiles_sketch_generic_normalized_rank_error<T>,
         py::arg("k"), py::arg("as_pmf"),
         "Gets the normalized rank error given parameters k and the pmf flag.\n"
         "If pmf is True, returns the 'double-sided' normalized rank error for the get_PMF() function.\n"
         "Otherwise, it is the 'single-sided' normalized rank error for all the other queries.\n"
         "Constants were derived as the best fit to 99 percentile empirically measured max error in thousands of trials")
    .def("serialize", &dspy::quantiles_sketch_serialize<T>, "Serializes the sketch into a bytes object")
    .def_static("deserialize", &dspy::quantiles_sketch_deserialize<T>, "Deserializes the sketch from a bytes object")
    ;
}

void init_quantiles(py::module &m) {
  bind_quantiles_sketch<int>(m, "quantiles_ints_sketch");
  bind_quantiles_sketch<float>(m, "quantiles_floats_sketch");
  bind_quantiles_sketch<double>(m, "quantiles_doubles_sketch");
}
