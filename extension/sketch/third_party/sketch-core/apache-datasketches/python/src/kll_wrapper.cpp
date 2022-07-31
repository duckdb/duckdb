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

#include "kll_sketch.hpp"

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
kll_sketch<T> kll_sketch_deserialize(py::bytes skBytes) {
  std::string skStr = skBytes; // implicit cast  
  return kll_sketch<T>::deserialize(skStr.c_str(), skStr.length());
}

template<typename T>
py::object kll_sketch_serialize(const kll_sketch<T>& sk) {
  auto serResult = sk.serialize();
  return py::bytes((char*)serResult.data(), serResult.size());
}

// maybe possible to disambiguate the static vs method rank error calls, but
// this is easier for now
template<typename T>
double kll_sketch_generic_normalized_rank_error(uint16_t k, bool pmf) {
  return kll_sketch<T>::get_normalized_rank_error(k, pmf);
}

template<typename T>
double kll_sketch_get_rank(const kll_sketch<T>& sk, const T& item, bool inclusive) {
  if (inclusive)
    return sk.template get_rank<true>(item);
  else
    return sk.template get_rank<false>(item);
}

template<typename T>
T kll_sketch_get_quantile(const kll_sketch<T>& sk,
                          double rank,
                          bool inclusive) {
  if (inclusive)
    return T(sk.template get_quantile<true>(rank));
  else
    return T(sk.template get_quantile<false>(rank));
}

template<typename T>
py::list kll_sketch_get_quantiles(const kll_sketch<T>& sk,
                                  std::vector<double>& fractions,
                                  bool inclusive) {
  size_t nQuantiles = fractions.size();
  auto result = inclusive ?
      sk.template get_quantiles<true>(fractions.data(), nQuantiles)
    : sk.template get_quantiles<false>(fractions.data(), nQuantiles);

  // returning as std::vector<> would copy values to a list anyway
  py::list list(nQuantiles);
  for (size_t i = 0; i < nQuantiles; ++i) {
      list[i] = result[i];
  }

  return list;
}

template<typename T>
py::list kll_sketch_get_pmf(const kll_sketch<T>& sk,
                            std::vector<T>& split_points,
                            bool inclusive) {
  size_t nPoints = split_points.size();
  auto result = inclusive ?
      sk.template get_PMF<true>(split_points.data(), nPoints)
    : sk.template get_PMF<false>(split_points.data(), nPoints);

  py::list list(nPoints + 1);
  for (size_t i = 0; i <= nPoints; ++i) {
    list[i] = result[i];
  }

  return list;
}

template<typename T>
py::list kll_sketch_get_cdf(const kll_sketch<T>& sk,
                            std::vector<T>& split_points,
                            bool inclusive) {
  size_t nPoints = split_points.size();
  auto result = inclusive ?
      sk.template get_CDF<true>(split_points.data(), nPoints)
    : sk.template get_CDF<false>(split_points.data(), nPoints);

  py::list list(nPoints + 1);
  for (size_t i = 0; i <= nPoints; ++i) {
    list[i] = result[i];
  }

  return list;
}

template<typename T>
void kll_sketch_update(kll_sketch<T>& sk, py::array_t<T, py::array::c_style | py::array::forcecast> items) {
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
void bind_kll_sketch(py::module &m, const char* name) {
  using namespace datasketches;

  py::class_<kll_sketch<T>>(m, name)
    .def(py::init<uint16_t>(), py::arg("k")=kll_constants::DEFAULT_K)
    .def(py::init<const kll_sketch<T>&>())
    .def("update", (void (kll_sketch<T>::*)(const T&)) &kll_sketch<T>::update, py::arg("item"),
         "Updates the sketch with the given value")
    .def("update", &dspy::kll_sketch_update<T>, py::arg("array"),
         "Updates the sketch with the values in the given array")
    .def("merge", (void (kll_sketch<T>::*)(const kll_sketch<T>&)) &kll_sketch<T>::merge, py::arg("sketch"),
         "Merges the provided sketch into the this one")
    .def("__str__", &kll_sketch<T>::to_string, py::arg("print_levels")=false, py::arg("print_items")=false,
         "Produces a string summary of the sketch")
    .def("to_string", &kll_sketch<T>::to_string, py::arg("print_levels")=false, py::arg("print_items")=false,
         "Produces a string summary of the sketch")
    .def("is_empty", &kll_sketch<T>::is_empty,
         "Returns True if the sketch is empty, otherwise False")
    .def("get_k", &kll_sketch<T>::get_k,
         "Returns the configured parameter k")
    .def("get_n", &kll_sketch<T>::get_n,
         "Returns the length of the input stream")
    .def("get_num_retained", &kll_sketch<T>::get_num_retained,
         "Returns the number of retained items (samples) in the sketch")
    .def("is_estimation_mode", &kll_sketch<T>::is_estimation_mode,
         "Returns True if the sketch is in estimation mode, otherwise False")
    .def("get_min_value", &kll_sketch<T>::get_min_value,
         "Returns the minimum value from the stream. If empty, kll_floats_sketch retursn nan; kll_ints_sketch throws a RuntimeError")
    .def("get_max_value", &kll_sketch<T>::get_max_value,
         "Returns the maximum value from the stream. If empty, kll_floats_sketch retursn nan; kll_ints_sketch throws a RuntimeError")
    .def("get_quantile", &dspy::kll_sketch_get_quantile<T>, py::arg("fraction"), py::arg("inclusive")=false,
         "Returns an approximation to the value of the data item "
         "that would be preceded by the given fraction of a hypothetical sorted "
         "version of the input stream so far.\n"
         "Note that this method has a fairly large overhead (microseconds instead of nanoseconds) "
         "so it should not be called multiple times to get different quantiles from the same "
         "sketch. Instead use get_quantiles(), which pays the overhead only once.\n"
         "For kll_floats_sketch: if the sketch is empty this returns nan. "
         "For kll_ints_sketch: if the sketch is empty this throws a RuntimeError.")
    .def("get_quantiles", &dspy::kll_sketch_get_quantiles<T>, py::arg("fractions"), py::arg("inclusive")=false,
         "This is a more efficient multiple-query version of get_quantile().\n"
         "This returns an array that could have been generated by using get_quantile() for each "
         "fractional rank separately, but would be very inefficient. "
         "This method incurs the internal set-up overhead once and obtains multiple quantile values in "
         "a single query. It is strongly recommend that this method be used instead of multiple calls "
         "to get_quantile().\n"
         "If the sketch is empty this returns an empty vector.")
    .def("get_rank", &dspy::kll_sketch_get_rank<T>, py::arg("value"), py::arg("inclusive")=false,
         "Returns an approximation to the normalized (fractional) rank of the given value from 0 to 1, inclusive.\n"
         "The resulting approximation has a probabilistic guarantee that can be obtained from the "
         "get_normalized_rank_error(False) function.\n"
         "With the parameter inclusive=true the weight of the given value is included into the rank."
         "Otherwise the rank equals the sum of the weights of values less than the given value.\n"
         "If the sketch is empty this returns nan.")
    .def("get_pmf", &dspy::kll_sketch_get_pmf<T>, py::arg("split_points"), py::arg("inclusive")=false,
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
    .def("get_cdf", &dspy::kll_sketch_get_cdf<T>, py::arg("split_points"), py::arg("inclusive")=false,
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
    .def("normalized_rank_error", (double (kll_sketch<T>::*)(bool) const) &kll_sketch<T>::get_normalized_rank_error,
         py::arg("as_pmf"),
         "Gets the normalized rank error for this sketch.\n"
         "If pmf is True, returns the 'double-sided' normalized rank error for the get_PMF() function.\n"
         "Otherwise, it is the 'single-sided' normalized rank error for all the other queries.\n"
         "Constants were derived as the best fit to 99 percentile empirically measured max error in thousands of trials")
    .def_static("get_normalized_rank_error", &dspy::kll_sketch_generic_normalized_rank_error<T>,
         py::arg("k"), py::arg("as_pmf"),
         "Gets the normalized rank error given parameters k and the pmf flag.\n"
         "If pmf is True, returns the 'double-sided' normalized rank error for the get_PMF() function.\n"
         "Otherwise, it is the 'single-sided' normalized rank error for all the other queries.\n"
         "Constants were derived as the best fit to 99 percentile empirically measured max error in thousands of trials")
    .def("serialize", &dspy::kll_sketch_serialize<T>, "Serializes the sketch into a bytes object")
    .def_static("deserialize", &dspy::kll_sketch_deserialize<T>, "Deserializes the sketch from a bytes object")
    ;
}

void init_kll(py::module &m) {
  bind_kll_sketch<int>(m, "kll_ints_sketch");
  bind_kll_sketch<float>(m, "kll_floats_sketch");
  bind_kll_sketch<double>(m, "kll_doubles_sketch");
}
