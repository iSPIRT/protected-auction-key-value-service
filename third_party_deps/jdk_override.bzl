"""JDK 21.48.15 CA override for Nessus CVE compliance.

Overrides default remotejdk21_linux with Azul Zulu 21.48.15 CA.
Must be called before any rule that loads the default JDK.
"""

load("@bazel_tools//tools/jdk:remote_java_repository.bzl", "remote_java_repository")

def jdk_21_override():
    remote_java_repository(
        name = "remotejdk21_linux",
        version = "21",
        target_compatible_with = ["@platforms//os:linux"],
        prefix = "remotejdk",
        urls = [
            "https://cdn.azul.com/zulu/bin/zulu21.48.15-ca-jdk21.0.10-linux_x64.tar.gz",
        ],
        sha256 = "7f15f667580a8977962dc0a709cf2a097cc71244614fad3f236debce9d1c2670",
        strip_prefix = "zulu21.48.15-ca-jdk21.0.10-linux_x64",
    )
