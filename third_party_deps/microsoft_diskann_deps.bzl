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

# libmkl-full-dev - mkl itself and oml (standart installation pathes PATH so we need to put omp.h and libomp manually)
def diskann_deps():
    native.new_local_repository(
        name = "mkl_headers",
        build_file_content = """
package(default_visibility = ["//visibility:public"])
cc_library(
    name = "headers",
    hdrs = glob(["**/*.h"]),
)
""",
        path = "/usr/include/mkl",
    )

    native.new_local_repository(
        name = "mkl_libs",
        build_file_content = """
package(default_visibility = ["//visibility:public"])
load("@rules_pkg//pkg:mappings.bzl", "pkg_attributes", "pkg_files")
cc_library(
    name = "libs",
    srcs = glob(["mkl/**/*.so", "mkl/**/*.so.3"]),
)
pkg_files(
    name = "libs_for_copy",
    srcs = [
        "libblas.so",
        "libblas64.so",
        "libblas64.so.3",
        "libquadmath.so.0",
        "libgfortran.so.5",
        "libmkl_rt.so",
        "liblapack.so",
        "liblapack64.so",
        "liblapack64.so.3",
    ],
    prefix = "/usr/lib/x86_64-linux-gnu",
    attributes = pkg_attributes(mode = "0444"),
)
""",
        path = "/usr/lib/x86_64-linux-gnu",
    )

    native.new_local_repository(
        name = "omp_headers",
        build_file_content = """
package(default_visibility = ["//visibility:public"])
cc_library(
    name = "headers",
    hdrs = glob(["**/*.h"]),
)
""",
        path = "/usr/lib/llvm-10/include/openmp",
    )

    native.new_local_repository(
        name = "omp_libs",
        build_file_content = """
package(default_visibility = ["//visibility:public"])
load("@rules_pkg//pkg:mappings.bzl", "pkg_attributes", "pkg_files")
cc_library(
    name = "libs",
    srcs = ["libomp.so", "libomp.so.5"],
)
pkg_files(
    name = "libs_for_copy",
    srcs = [
        "libomp.so",
        "libomp.so.5",
    ],
    prefix = "/usr/lib/x86_64-linux-gnu",
    attributes = pkg_attributes(mode = "0444"),
)
""",
        path = "/usr/lib/llvm-10/lib",
    )
