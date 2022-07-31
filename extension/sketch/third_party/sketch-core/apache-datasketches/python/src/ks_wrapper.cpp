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

#include "kolmogorov_smirnov.hpp"
#include "kll_sketch.hpp"
#include "quantiles_sketch.hpp"

#include <pybind11/pybind11.h>

namespace py = pybind11;

void init_kolmogorov_smirnov(py::module &m) {
  using namespace datasketches;

  m.def("ks_test", &kolmogorov_smirnov::test<kll_sketch<int>>, py::arg("sk_1"), py::arg("sk_2"), py::arg("p"),
    "Performs the Kolmogorov-Smirnov Test between kll_ints_sketches.\n"
    "Note: if the given sketches have insufficient data or if the sketch sizes are too small, "
    "this will return false.\n"
    "Returns True if we can reject the null hypothesis (that the sketches reflect the same underlying "
    "distribution) using the provided p-value, otherwise False.");
  m.def("ks_test", &kolmogorov_smirnov::test<kll_sketch<float>>, py::arg("sk_1"), py::arg("sk_2"), py::arg("p"),
    "Performs the Kolmogorov-Smirnov Test between kll_floats_sketches.\n"
    "Note: if the given sketches have insufficient data or if the sketch sizes are too small, "
    "this will return false.\n"
    "Returns True if we can reject the null hypothesis (that the sketches reflect the same underlying "
    "distribution) using the provided p-value, otherwise False.");
  m.def("ks_test", &kolmogorov_smirnov::test<kll_sketch<double>>, py::arg("sk_1"), py::arg("sk_2"), py::arg("p"),
    "Performs the Kolmogorov-Smirnov Test between kll_doubles_sketches.\n"
    "Note: if the given sketches have insufficient data or if the sketch sizes are too small, "
    "this will return false.\n"
    "Returns True if we can reject the null hypothesis (that the sketches reflect the same underlying "
    "distribution) using the provided p-value, otherwise False.");

  m.def("ks_test", &kolmogorov_smirnov::test<quantiles_sketch<int>>, py::arg("sk_1"), py::arg("sk_2"), py::arg("p"),
    "Performs the Kolmogorov-Smirnov Test between quantiles_ints_sketches.\n"
    "Note: if the given sketches have insufficient data or if the sketch sizes are too small, "
    "this will return false.\n"
    "Returns True if we can reject the null hypothesis (that the sketches reflect the same underlying "
    "distribution) using the provided p-value, otherwise False.");
  m.def("ks_test", &kolmogorov_smirnov::test<quantiles_sketch<float>>, py::arg("sk_1"), py::arg("sk_2"), py::arg("p"),
    "Performs the Kolmogorov-Smirnov Test between quantiles_floats_sketches.\n"
    "Note: if the given sketches have insufficient data or if the sketch sizes are too small, "
    "this will return false.\n"
    "Returns True if we can reject the null hypothesis (that the sketches reflect the same underlying "
    "distribution) using the provided p-value, otherwise False.");
  m.def("ks_test", &kolmogorov_smirnov::test<quantiles_sketch<double>>, py::arg("sk_1"), py::arg("sk_2"), py::arg("p"),
    "Performs the Kolmogorov-Smirnov Test between quantiles_doubles_sketches.\n"
    "Note: if the given sketches have insufficient data or if the sketch sizes are too small, "
    "this will return false.\n"
    "Returns True if we can reject the null hypothesis (that the sketches reflect the same underlying "
    "distribution) using the provided p-value, otherwise False.");
}
