# Build Architecture & Vulnerability Upgrade Reference

## Repo Structure

```
.                                       # Root Bazel workspace (key-value service)
├── .bazelversion                       # Bazel version for build (via bazelisk)
├── .bazelrc                            # Bazel flags, platform/instance/mode configs
├── WORKSPACE                           # External deps: servers_common, grpc, rules_oci, etc.
├── BUILD.bazel                        # Root: platform, instance, mode string_flags; config_setting groups
├── third_party_deps/
│   ├── cpp_repositories.bzl           # C++ externals: riegeli, zstd, flatbuffers, roaring_bitmap, etc.
│   ├── container_deps.bzl             # Runtime images: loads builders/bazel/container_deps.bzl; adds envoy-distroless, envoy_binary, aws-lambda-python
│   ├── microsoft_diskann_deps.bzl     # MKL/OpenMP for DiskANN (Azure ANN index)
│   └── *.BUILD, *.patch                # Build files and patches for third-party libs
├── builders/
│   ├── bazel/
│   │   ├── container_deps.bzl         # Base runtime images: distroless/cc-debian12 (debug/nondebug, root/nonroot)
│   │   └── deps.bzl                   # Python deps, toolchains
│   └── tools/
│       ├── builder.sh                 # cbuild_debian, workspace mount helpers
│       ├── pre-commit, buildifier      # Pre-commit hooks
│       └── normalize-dist, collect-logs, etc.
├── components/                        # Core C++ server components
│   ├── data_server/                   # server binary, request_handler, cache, data_loading, microsoft_ann_index
│   ├── internal_server/               # lookup, local_lookup, sharded_lookup, microsoft_ann_lookup
│   ├── udf/, udf/hooks/               # UDF client, ROMA hooks (get_values, run_query, microsoft_get_nearest_neighbors)
│   ├── query/                         # Bison/Flex parser, driver, scanner, ast (query language)
│   ├── cloud_config/                  # parameter_client, instance_client (platform-selected)
│   ├── data/blob_storage/             # blob_storage_client, change_notifier (platform-selected)
│   ├── sharding/                      # shard_manager, cluster_mappings_manager
│   ├── telemetry/, errors/, util/, container/, ...
│   └── ...
├── public/                            # APIs, protos, shared types
│   ├── query/                         # get_values proto, get_values_v2, query_api_descriptor_set
│   ├── data_loading/, sharding/, udf/, applications/pa/, test_util/
│   └── ...
├── production/packaging/
│   ├── build_and_test_all_in_docker   # Top-level build orchestrator (single Bazel workspace)
│   ├── azure/                         # Azure packaging: data_server (oci_image), build_and_test
│   ├── aws/                           # AWS packaging: data_server, AMI, build_and_test
│   ├── gcp/                           # GCP packaging: data_server, build_and_test
│   ├── local/                         # Local packaging: data_server, build_and_test
│   └── tools/                         # request_simulation, otel_collector, etc.
├── testing/
│   ├── functionaltest/                # Functional tests (this repo)
│   └── functionaltest-system/         # local_repository: independent test framework (own WORKSPACE)
└── tools/                             # data_cli, benchmarking, udf tools, request_simulation, etc.
```

**Key point:** The repo has **one main Bazel workspace** plus one **local_repository** (functionaltest-system) used for functional test infrastructure. There are no separate inference sidecar workspaces. Platform (aws / azure_microsoft / gcp / local) and instance/mode are selected via `--config` and root `string_flag`s.

## Build Process Layers

A single build command orchestrates **5 distinct layers**, each introducing its own dependency surface:

```
┌─────────────────────────────────────────────────────────┐
│  Layer 1: Host VM                                       │
│  └─ bazelisk (or system Bazel), Docker daemon           │
│  └─ Bazel cache at ~/.cache/bazel (optional volume      │
│     mount into build container)                         │
├─────────────────────────────────────────────────────────┤
│  Layer 2: Build Container (build-debian Docker image)   │
│  └─ Clang, Python, system libs for compilation          │
│  └─ /bazel_root mounted from host ~/.cache/bazel        │
│  └─ /src/workspace mounted from host repo               │
├─────────────────────────────────────────────────────────┤
│  Layer 3: Bazel (controlled by .bazelversion)           │
│  └─ Embedded JDK (Bazel 7 → JDK 21)                     │
│  └─ Remote JDK toolchain (remotejdk_21 in .bazelrc)     │
│  └─ All artifacts land in host cache via volume mount   │
├─────────────────────────────────────────────────────────┤
│  Layer 4: WORKSPACE Dependencies                        │
│  └─ google_privacysandbox_servers_common (cpp_deps,     │
│     deps1–deps4) → grpc, protobuf, abseil, ROMA, etc.   │
│  └─ third_party_deps: cpp_repositories, container_deps  │
│  └─ rules_oci, rules_pkg, rules_cc, rules_proto,        │
│     rules_bison, rules_flex, pybind11_bazel, etc.       │
│  └─ microsoft_diskann (optional, for Azure ANN index)   │
├─────────────────────────────────────────────────────────┤
│  Layer 5: Runtime Container Images                      │
│  └─ Base: gcr.io/distroless/cc-debian12 (builders/      │
│     bazel/container_deps.bzl)                           │
│  └─ Envoy: envoyproxy/envoy-distroless or envoy_binary  │
│     (third_party_deps/container_deps.bzl)               │
│  └─ Azure image also: diskann_deps_tar (MKL/OpenMP)     │
│  └─ Flattened into final Docker image via rules_oci     │
│  └─ These determine glibc/openssl in shipped images     │
└─────────────────────────────────────────────────────────┘
```

## Build Execution Flow

```
build_and_test_all_in_docker  [--platform aws|azure_microsoft|gcp|local] [--instance ...] [--mode prod|nonprod]
│
├── Pre-commit (unless --no-precommit)
│   └─ builders/tools/pre-commit
│
├── Single-phase build (via builder::cbuild_debian → Docker(build-debian) → Bazel)
│   ├─ BAZEL_EXTRA_ARGS = --config=${INSTANCE}_instance --config=${PLATFORM}_platform --config=${MODE}_mode
│   ├─ bazel build //components/... //public/... //tools/...
│   ├─ [optional] bazel test --build_tests_only //...
│   └─ Platform-specific copy_to_dist:
│       ├─ gcp     → //production/packaging/gcp/data_server:copy_to_dist
│       ├─ local   → //production/packaging/local/data_server:copy_to_dist + tools:copy_to_dist
│       ├─ aws     → //production/packaging/aws/data_server:copy_to_dist
│       └─ azure_microsoft → //production/packaging/azure/data_server:copy_to_dist
│   └─ bazel run //testing/functionaltest:copy_to_dist
│
└── Artifacts in dist/
    ├─ key_value_service_image.tar (or platform-specific path)
    ├─ query_api_descriptor_set.pb
    ├─ debian/ (server artifacts, symlinks)
    └─ test_data/ (for local/functional tests)
```

**Critical detail:** There is only one Bazel workspace for the main service. Platform and mode select which **source files and which packaging target** are built (e.g. parameter_fetcher_azure.cc vs _aws.cc, and which data_server:copy_to_dist runs). The same .bazelversion applies to the entire build.

## Vulnerability Surface Map

| Vuln Surface | Controlled By | Affects |
|---|---|---|
| glibc, libssl in shipped images | `builders/bazel/container_deps.bzl` (distroless digests), `third_party_deps/container_deps.bzl` (envoy digests) | CVE scans of Docker images |
| Embedded JDK in Bazel | `.bazelversion` | VAPT scans of build VM |
| Remote JDK toolchain | `.bazelrc` `--java_runtime_version` | VAPT scans of build VM |
| OpenSSL/BoringSSL in build | WORKSPACE → servers_common → grpc/boringssl | VAPT scans of build VM |
| Build container OS packages | `builders/images/build-debian/` (if present) or host | VAPT scans of build VM |
| Transitive C++ deps | WORKSPACE → servers_common (cpp_deps, deps1–4), `third_party_deps/cpp_repositories.bzl` | Build-time supply chain |
| MKL/OpenMP (Azure DiskANN) | `third_party_deps/microsoft_diskann_deps.bzl`, system paths | Azure builds only; build VM / image |

## Upgrade Protocol

### Step 0: Identify the Layer

Map each CVE to its layer using the table above. Don't shotgun — a glibc CVE in the Docker image is Layer 5, not Layer 3.

### Step 1: Runtime Image Vulnerabilities (Layer 5)

**Files:** `builders/bazel/container_deps.bzl`, `third_party_deps/container_deps.bzl`

1. Identify which image carries the vuln (distroless/cc-debian12 base or envoy-distroless).
2. Find updated image digests (GCR / Docker Hub; match amd64/arm64).
3. Update the digest values in the corresponding `_images` / `images` dict. If switching Debian generations (e.g. 11→12), update registry/repository if needed.
4. **Watch for:** Envoy image is layered on top of distroless in the final image. If both carry conflicting libs, the envoy layer can win. Prefer upgrading both to the same Debian generation.

### Step 2: Bazel / JDK Vulnerabilities (Layer 3)

**Files:** `.bazelversion`, `.bazelrc`, root `BUILD.bazel`

1. Update `.bazelversion` (single workspace).
2. Add compatibility flags in `.bazelrc` if needed:
   - e.g. `common --noenable_bzlmod`, `build --features=-parse_headers` for major version jumps.
   - JDK: `build --java_runtime_version=remotejdk_NN`, `build --tool_java_runtime_version=remotejdk_NN`.
3. **Watch for:** Platform/instance/mode configs in .bazelrc mirror flags into `@google_privacysandbox_servers_common`. Ensure any flag renames stay in sync.

### Step 3: Transitive Dependencies (Layer 4)

**google_privacysandbox_servers_common:** Loaded in WORKSPACE; supplies cpp_deps, deps1–deps4. Many components depend on it (ROMA, key_fetcher, telemetry, retry, logger, CPIO for GCP). Upgrading it is a cross-repo change; coordinate with the shared library’s version and API.

**C++ third-party (this repo):** `third_party_deps/cpp_repositories.bzl` defines riegeli, zstd, snappy, flatbuffers, sqlite3, benchmark, roaring_bitmap, etc. Update http_archive URLs and sha256. Patches live in `third_party_deps/*.patch` and `*.BUILD`.

**rules_oci / rules_pkg:** Used for OCI images and packaging. Version bumps can change Starlark APIs; test `bazel build //production/packaging/azure/data_server:server_docker_image` (or equivalent).

### Step 4: Platform / Instance / Mode (Rigidity)

**Files:** `BUILD.bazel` (root), `.bazelrc`, all packages that use `select()` on `//:platform`, `//:instance`, `//:mode`

Adding a new platform (e.g. new cloud) requires:
1. New `string_flag` values and `config_setting` in root `BUILD.bazel`.
2. New `--config xxx_platform` and `xxx_instance` in `.bazelrc`, plus mirror to `@google_privacysandbox_servers_common`.
3. New branches in every `select()` that keys off platform/instance/mode: e.g. `components/data_server/server` (parameter_fetcher, key_fetcher_factory, server_log_init), `components/cloud_config`, `components/data/blob_storage`, `components/sharding`.
4. New or updated `production/packaging/<platform>/data_server` and `copy_to_dist` wiring in `build_and_test_all_in_docker`.

### Step 5: Verify

After any change:

```bash
# Full build (example Azure prod)
./production/packaging/build_and_test_all_in_docker \
  --platform azure_microsoft --instance local --mode prod

# Or build directly (no Docker wrapper) if Bazel is on path:
bazel build --config=azure_microsoft_platform --config=local_instance --config=prod_mode \
  //production/packaging/azure/data_server:server_docker_image

# Verify runtime image (after load from dist or tarball)
docker run --rm --entrypoint="" <image> sh -c 'cat /etc/os-release'
docker run --rm --entrypoint="" <image> sh -c '/lib/x86_64-linux-gnu/libc.so.6 2>&1 | head -1'
```

### Gotchas & Lessons Learned

1. **Platform/mode are wired in many places.** Root BUILD.bazel defines `platform`, `instance`, `mode` and config_setting groups (e.g. `aws_prod`, `azure_microsoft_platform`). .bazelrc mirrors these into servers_common. Any new platform needs root flags, .bazelrc, and every select() that branches on platform/instance/mode.

2. **server_lib is a dependency hub.** `//components/data_server/server:server_lib` depends on request_handler, cache, data_loading, internal_server, udf, sharding, cloud_config, telemetry, and public protos. Changes to any of these can force long rebuilds and test runs.

3. **UDF and ROMA live in servers_common.** Components under `//components/udf` and `//components/udf/hooks` depend on `@google_privacysandbox_servers_common//src/roma`. ROMA version is tied to the shared library; upgrading ROMA means upgrading servers_common.

4. **Azure image adds DiskANN/MKL.** `production/packaging/azure/data_server` includes diskann_deps_tar (MKL/OpenMP). Builds that produce the Azure image depend on microsoft_diskann and diskann_deps (mkl_libs, omp_libs). Ensure build environment has MKL/OpenMP or the paths configured in microsoft_diskann_deps.bzl.

5. **Query API descriptor is shared.** `//public/query:query_api_descriptor_set` aggregates get_values, get_values_v2, and health protos. It is copied into dist and used by Envoy/config. Proto or service renames require descriptor and any config that references it to be updated.

6. **Single workspace, single .bazelversion.** Unlike repos with multiple sidecar workspaces, here one Bazel version and one WORKSPACE govern the whole build. Upgrading Bazel touches everything; run full build and key tests after the upgrade.
