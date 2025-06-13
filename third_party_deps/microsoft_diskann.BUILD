# Copyright (c) Microsoft Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

load("@rules_cc//cc:defs.bzl", "cc_library")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "diskann_library",
    srcs = glob(["src/*.cpp"]),
    hdrs = glob(["include/*.h", "include/tsl/*.h"]),
    includes = ["include", "include/tsl"],
    copts = [
        "-ltcmalloc", "-Ofast", "-march=native", "-mtune=native",
        "-Iexternal/mkl_headers", "-Iexternal/omp_headers",
        "-Lexternal/mkl_libs", "-Lexternal/omp_libs",
        "-m64", "-Wl,--no-as-needed",
        "-DMKL_ILP64", "-DNDEBUG", "-DUSE_AVX2",
        "-mavx2", "-mfma", "-msse2",
        "-ftree-vectorize", "-fno-builtin-malloc", "-fno-builtin-calloc", "-fno-builtin-realloc",
        "-fno-builtin-free", "-fopenmp", "-fopenmp-simd",
        "-funroll-loops", '-Wfatal-errors'
    ],
    deps = [
        "@boost//:dynamic_bitset",
        "@mkl_headers//:headers",
        "@mkl_libs//:libs",
        "@omp_headers//:headers",
        "@omp_libs//:libs",
    ],
    linkstatic = True,
    alwayslink = True,
)
