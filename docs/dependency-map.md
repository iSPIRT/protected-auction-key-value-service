# Dependency Map & Coupling Analysis

> **Purpose:** Comprehensive reference for understanding what depends on what, where
> tight bindings exist, and what breaks when you change something. Designed for
> quick interpretation by both humans and AI agents.

---

## 1. Build Orchestration Graph

```
build_and_test_all_in_docker  [--platform P] [--instance I] [--mode M]
│
│  ┌──────────────────── SINGLE PHASE: Main Build ──────────────────────┐
│  │  (one Bazel workspace, via cbuild_debian → Docker(build-debian))    │
│  │                                                                     │
│  │  Root workspace                                                     │
│  │  ├─ .bazelversion (7.4.1)                                          │
│  │  ├─ .bazelrc  (--config=${INSTANCE}_instance --config=${PLATFORM}_platform --config=${MODE}_mode)
│  │  ├─ WORKSPACE  (servers_common, rules_oci, grpc, third_party_deps)  │
│  │  ├─ builders/bazel/container_deps.bzl  ← base runtime images      │
│  │  ├─ third_party_deps/container_deps.bzl ← envoy, lambda images     │
│  │  ├─ bazel build //components/... //public/... //tools/...          │
│  │  └─ bazel run .../copy_to_dist  (per platform)                     │
│  │       → dist/key_value_service_image.tar (or platform path)         │
│  └─────────────────────────────────────────────────────────────────────┘
│                          ↓
│  ┌──────────────────── Platform-specific packaging ───────────────────┐
│  │  azure_microsoft | aws | gcp | local                               │
│  │  (copy_to_dist selects which data_server target runs)              │
│  └─────────────────────────────────────────────────────────────────────┘

Shared:
  Host: ~/.cache/bazel  ←──volume-mount──→  Container: /bazel_root  (optional)
  Host: repo root       ←──volume-mount──→  Container: /src/workspace
```

---

## 2. Root Workspace Dependency Tree

```
ROOT WORKSPACE (name = google_privacysandbox_kv_server, .bazelversion=7.4.1)
│
├── local_repository: google_privacysandbox_functionaltest_system
│   └─ path: testing/functionaltest-system  (own WORKSPACE, used for functional test framework)
│
├── google_privacysandbox_servers_common   ← DATA PLANE SHARED LIB (large dep tree)
│   ├→ cpp_dependencies()                  ← grpc, protobuf, abseil, boringssl, etc.
│   ├→ deps1()                             ← rules_apple, rules_go, upb, ...
│   ├→ deps2(go_toolchains_version=...)    ← go toolchains, gazelle
│   ├→ deps3()                             ← rules_js, rules_ts, aspect_*
│   └→ deps4()                             ← additional transitive deps
│
├── third_party_deps/cpp_repositories.bzl  ← C++ EXTERNALS
│   ├─ com_google_riegeli, net_zstd, snappy, highwayhash
│   ├─ com_github_google_flatbuffers, sqlite3
│   ├─ com_google_benchmark, roaring_bitmap
│   └─ (patches and build files in third_party_deps/)
│
├── third_party_deps/container_deps.bzl     ← RUNTIME IMAGES (extends builders)
│   ├─ load builders/bazel/container_deps.bzl  (distroless/cc-debian12 base images)
│   ├─ aws-lambda-python, envoy-distroless (amd64/arm64 digests)
│   └─ envoy_binary (http_file, for GCP)
│
├── third_party_deps/microsoft_diskann_deps.bzl  ← MKL/OpenMP (Azure DiskANN)
│   └─ mkl_headers, mkl_libs, omp_libs (local or system paths)
│
├── com_google_googleapis (pinned commit)
├── distributed_point_functions, libcbor (patched)
├── rules_m4, rules_bison, rules_flex       ← Bison/Flex for components/query
├── io_bazel_rules_go, container_structure_test
├── pybind11_bazel → python_configure
├── microsoft_diskann (http_archive or local, for Azure ANN index)
├── latency_benchmark, word2vec (pip/requirements)
└── buf, rules_buf, rules_proto, rules_cc, rules_oci, rules_pkg (via servers_common or direct)
```

---

## 3. Server Binary & Packaging Dependency Tree

```
production/packaging/<platform>/data_server:server_docker_image
│   (e.g. azure/data_server, aws/data_server, gcp/data_server, local/data_server)
│
├── oci_image(
│       base = select(arm64|x86_64) → runtime-debian-debug-root-*  (builders/bazel/container_deps.bzl)
│       tars = [ server_binaries_tar, envoy_binary_tar, envoy_config_tar(, diskann_deps_tar for Azure) ]
│   )
│
├── server_binaries_tar
│   └─ bin/init_server_basic, components/data_server/server (binary), jq
│
├── components/data_server/server (cc_binary "server")
│   ├─ main.cc
│   └─ deps: server_lib, key_fetcher_factory, server_log_init, shard_manager, version_linkstamp, absl, servers_common
│
├── server_lib  ═══════════════════════════════════════════════════════════════╗
│   ├─ key_value_service_impl, key_value_service_v2_impl                       ║
│   ├─ parameter_fetcher (select platform), lifecycle_heartbeat                 ║  TIGHT COUPLING:
│   ├─ key_fetcher_factory (select platform × mode), server_initializer        ║  single hub for
│   ├─ components/data_server/cache, request_handler (get_values_*, adapter)   ║  almost all
│   ├─ components/data_server/data_loading (data_orchestrator)                  ║  components
│   ├─ components/internal_server (lookup, local_lookup, sharded_lookup,       ║
│   │   lookup_server_impl, microsoft_ann_lookup)                               ║
│   ├─ components/udf (udf_client, udf_config_builder), udf/hooks               ║
│   ├─ components/sharding (cluster_mappings_manager), cloud_config             ║
│   ├─ components/telemetry, util, microsoft_ann_index                          ║
│   ├─ public (base_types_cc_proto, constants, query get_values*, sharding,     ║
│   │   data_loading readers, applications/pa, udf constants)                  ║
│   └─ @com_github_grpc_grpc, @google_privacysandbox_servers_common (telemetry, ║
│       retry, periodic_closure, key_fetcher, ROMA), @com_google_absl           ║
│   ════════════════════════════════════════════════════════════════════════════╝
│
├── envoy_config_tar
│   └─ public/query:query_api_descriptor_set, envoy.yaml
│
└── (Azure only) diskann_deps_tar
    └─ @mkl_libs//:libs_for_copy, @omp_libs//:libs_for_copy
```

---

## 4. Key Component Dependency Tree (Simplified)

```
components/data_server/request_handler
├─ get_values_handler → get_values_adapter, cache, public/query get_values_cc_grpc
├─ get_values_v2_handler → get_values_adapter, cache, compression, content_type, encryption (ohttp),
│   partitions, status, udf_client, public/query/v2, public/applications/pa, quiche, servers_common
└─ get_values_adapter → get_values_v2_handler, content_type/encoder, public/applications/pa, get_values_cc_grpc, get_values_v2_cc_grpc

components/internal_server
├─ lookup, local_lookup, sharded_lookup, lookup_server_impl
├─ microsoft_ann_lookup → microsoft_ann_index, lookup
├─ local_lookup → cache, query (driver, scanner), errors
├─ sharded_lookup → local_lookup, remote_lookup_client_impl, query, shard_manager, public/sharding
└─ remote_lookup_client_impl → ohttp_client_encryptor, internal_lookup_cc_grpc

components/udf
├─ udf_client → code_config, udf/hooks (get_values_hook, run_query_hook, microsoft_get_nearest_neighbors_hook),
│   @google_privacysandbox_servers_common//src/roma (interface, roma_service)
└─ udf_config_builder → code_config, same hooks, ROMA

components/query (Bison/Flex query language)
├─ parser (bison_cc_library), scanner (flex_cc_library), driver, ast, sets
└─ Used by: internal_server (local_lookup, lookup_server_impl, sharded_lookup), udf/hooks

components/cloud_config
├─ parameter_client = select(aws|azure_microsoft|gcp|local) → parameter_client_aws/_azure/_gcp/_local
└─ instance_client = select(aws|azure_microsoft|gcp|local) → instance_client_* (AWS SDK, GCP CPIO, etc.)

components/data/blob_storage
├─ blob_storage_client = select(platform) → _s3, _gcp, _local
└─ blob_storage_change_notifier = select(platform) → _s3, _gcp, _local
```

---

## 5. Runtime Docker Image Layer Stack

```
FINAL SERVICE DOCKER IMAGE (e.g. key_value_service_image.tar for Azure)
┌─────────────────────────────────────────────────────────┐
│  Service binary layer                                    │  ← Bazel-built C++ binary
│  (init_server_basic + server binary + jq)                │
├─────────────────────────────────────────────────────────┤
│  (Azure only) diskann_deps_tar                           │  ← MKL/OpenMP libs
│  (MKL, OpenMP .so files)                                 │
├─────────────────────────────────────────────────────────┤
│  envoy_binary_tar / envoy-distroless                     │  ← third_party_deps/container_deps.bzl
│  Envoy proxy binary                                      │
│  ⚠ If envoy image has its own base, that layer wins on   │
│     path conflicts when flattened                        │
├─────────────────────────────────────────────────────────┤
│  distroless/cc-debian12 (BASE)                           │  ← builders/bazel/container_deps.bzl
│  (runtime-debian-debug-root for Azure)                   │
│  Minimal Debian 12 runtime, glibc, libstdc++, no shell   │
└─────────────────────────────────────────────────────────┘

⚠  Base and Envoy should align on Debian generation where both contribute libs.
```

---

## 6. Shared Bazel Cache Structure (Single Workspace)

```
~/.cache/bazel/  (HOST)  ←──volume-mount──→  /bazel_root/  (CONTAINER, if used)
│
└── {output_user_root}/
    ├── install/
    │   └── {bazel_version_hash}/     ← Bazel install base
    │       └── embedded_tools/jdk/    ← EMBEDDED JDK (Bazel 7 → JDK 21)
    ├── cache/
    └── {output_base_hash}/
        └── external/                 ← WORKSPACE deps (servers_common, grpc, etc.)

Single workspace → one output user root for the main repo.
functionaltest-system is a local_repository; its deps are still under the same
Bazel invocation when building targets that depend on it.
```

---

## 7. Tight Coupling & Rigidity Map

```
LEGEND:  ═══ Tight coupling (version-locked or many select branches)
         ─── Loose coupling (independently upgradable)
         ⚠   Known rigidity / upgrade hazard

═══════════════════════════════════════════════════════════════
COUPLING ZONE 1: Bazel ↔ Starlark & flags
═══════════════════════════════════════════════════════════════

  .bazelversion ═══ Bazel binary ═══ Embedded JDK version
       │                  │
       │            ═══ Starlark API (rules_oci, rules_pkg, etc.)
       │                  │
       ╠═══ .bazelrc flags (bzlmod, parse_headers, platform/instance/mode)
       ╠═══ Root BUILD.bazel (platform, instance, mode string_flags)
       └═══ Mirror to @google_privacysandbox_servers_common (platform, instance, build_flavor) ⚠

  RIGIDITY: Changing .bazelversion can break externals. Adding a platform
  requires root flags + .bazelrc + mirror + every select() that keys off them.

═══════════════════════════════════════════════════════════════
COUPLING ZONE 2: server_lib ↔ All core components (HIGHEST RIGIDITY)
═══════════════════════════════════════════════════════════════

  components/data_server/server:server_lib
       │
       ╠═══ request_handler (get_values_handler, get_values_v2_handler, get_values_adapter)
       ╠═══ cache, data_loading/data_orchestrator
       ╠═══ internal_server (lookup, local_lookup, sharded_lookup, microsoft_ann_lookup)
       ╠═══ udf (udf_client, udf_config_builder), udf/hooks
       ╠═══ sharding, cloud_config, telemetry
       ╠═══ microsoft_ann_index
       ╠═══ public (protos, query, sharding, data_loading, applications/pa, udf)
       └═══ servers_common (telemetry, retry, key_fetcher, ROMA)

  RIGIDITY: Any new first-class feature that server_lib uses adds to this hub.
  Refactors (e.g. splitting handlers) must preserve BUILD deps and visibility.

═══════════════════════════════════════════════════════════════
COUPLING ZONE 3: Platform / instance / mode select()
═══════════════════════════════════════════════════════════════

  //:platform (aws | azure_microsoft | gcp | local)
  //:instance (same set)
  //:mode (prod | nonprod)
       │
       ╠═══ parameter_fetcher (platform) ⚠
       ╠═══ key_fetcher_factory (platform × mode: aws_prod, aws_nonprod, azure_microsoft_*, gcp_*, local)
       ╠═══ server_log_init (mode × instance)
       ╠═══ parameter_client, instance_client (platform/instance)
       ╠═══ blob_storage_client, blob_storage_change_notifier (platform)
       ╠═══ cluster_mappings_manager (gcp vs default/aws)
       └═══ remote_lookup_client_impl (aws/gcp copts)

  RIGIDITY: New platform = new config_setting + .bazelrc + mirror + N select() branches.
  Missing one place yields wrong binary or build failure.

═══════════════════════════════════════════════════════════════
COUPLING ZONE 4: google_privacysandbox_servers_common ↔ Many components
═══════════════════════════════════════════════════════════════

  google_privacysandbox_servers_common (pinned archive)
       │
       ╠═══ ROMA (udf_client, udf/hooks) ⚠
       ╠═══ key_fetcher, encryption (key_fetcher_factory, parameter_fetcher, request_handler/encryption)
       ╠═══ telemetry, logger, retry, status_macro
       ╠═══ CPIO (GCP parameter_client, instance_client)
       ╠═══ request_context_logger, duration, sleep
       │
       └→ Transitively: grpc, protobuf, abseil, boringssl, rules_*, sandboxed_api, etc.

  RIGIDITY: Single version for entire repo. Upgrading it changes transitive deps
  for server, request_handler, internal_server, udf, cloud_config, blob_storage, cache.

═══════════════════════════════════════════════════════════════
COUPLING ZONE 5: internal_server ↔ components/query (Bison/Flex)
═══════════════════════════════════════════════════════════════

  components/query (parser.yy, scanner.ll → driver, scanner, parser, ast, sets)
       │
       ╠═══ local_lookup, lookup_server_impl, sharded_lookup (all use driver, scanner)
       ╠═══ udf/hooks (get_values_hook, run_query_hook use request_context; hooks call into lookup)
       └═══ Roaring bitmap (ast), rules_bison, rules_flex

  RIGIDITY: Query grammar/lexer changes affect all lookup paths. Bison/Flex
  generate .cc; version bumps of rules_bison/rules_flex can change generated API.

═══════════════════════════════════════════════════════════════
COUPLING ZONE 6: Runtime images ↔ Debian generation
═══════════════════════════════════════════════════════════════

  builders/bazel/container_deps.bzl
       ╠═══ distroless/cc-debian12  ═══ glibc version ═══ ABI compat with service binary
       ╠═══ third_party_deps/container_deps.bzl: envoy-distroless
       └── Envoy layer must match distroless Debian generation when both supply libs.
```

---

## 8. Upgrade Risk Matrix

| What You Change | Direct Impact | Transitive Risk | Rollback Ease |
|---|---|---|---|
| `container_deps.bzl` digests (builders + third_party_deps) | Runtime image libs | Low | Easy (revert digests) |
| `.bazelversion` | JDK, Starlark API | Medium (rules_oci, rules_cc, etc.) | Easy |
| Root `platform`/`instance`/`mode` flags | All select() branches | **High** (many packages) | Medium (many files) |
| `google_privacysandbox_servers_common` | All consumers of ROMA, key_fetcher, telemetry, etc. | **High** | Medium |
| `third_party_deps/cpp_repositories.bzl` | C++ libs (riegeli, flatbuffers, roaring, etc.) | Medium | Easy |
| `microsoft_diskann` / diskann_deps | Azure ANN index, Azure image | Medium (Azure path only) | Medium |
| `components/data_server/server` (server_lib deps) | Server binary | **High** | Medium |
| `public/query` protos / query_api_descriptor_set | Envoy config, clients | Medium | Easy (descriptor + config) |
| Bison/Flex or components/query | All lookup and hook paths | **High** | Medium |
| `key_fetcher_factory` or platform-select files | Correct key/parameter impl per platform | **High** | Medium |

---

## 9. Dependency Version Quick Reference

| Dependency | Version/Ref | Defined In | Used By |
|---|---|---|---|
| Bazel | 7.4.1 | `.bazelversion` | Whole workspace |
| JDK (embedded) | (Bazel 7 default) | Bazel binary | Build process |
| JDK (remote toolchain) | remotejdk_21 | `.bazelrc` | Build process |
| distroless base | cc-debian12 | `builders/bazel/container_deps.bzl` | Runtime images |
| Envoy | v1.31.4 (distroless image), v1.24.1 (binary for GCP) | `third_party_deps/container_deps.bzl` | Runtime / packaging |
| google_privacysandbox_servers_common | main branch archive | WORKSPACE | ROMA, key_fetcher, telemetry, logger, retry, CPIO |
| gRPC | via servers_common | WORKSPACE (servers_common) | Server, request_handler, internal_server |
| protobuf | via servers_common / rules_proto | WORKSPACE | All protos |
| rules_oci, rules_pkg | via servers_common or WORKSPACE | WORKSPACE | production/packaging |
| rules_bison, rules_flex | 3.3.2, 2.6.4 | WORKSPACE | components/query |
| com_google_riegeli, flatbuffers, roaring_bitmap, etc. | (see cpp_repositories.bzl) | `third_party_deps/cpp_repositories.bzl` | public/data_loading, components |
| microsoft_diskann | (pinned zip/archive) | WORKSPACE | components/data_server/microsoft_ann_index |
| MKL / OpenMP | system or local paths | `third_party_deps/microsoft_diskann_deps.bzl` | Azure build, diskann_deps_tar |
| Quiche | via servers_common | WORKSPACE | request_handler/encryption, internal_server (OHTTP) |
