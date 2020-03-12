load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["memory_manager.go"],
    importpath = "k8s.io/kubernetes/pkg/kubelet/cm/memorymanager",
    visibility = ["//visibility:public"],
    deps = [
        "//pkg/kubelet/cm/cpumanager/containermap:go_default_library",
        "//pkg/kubelet/cm/memorymanager/state:go_default_library",
        "//pkg/kubelet/cm/topologymanager:go_default_library",
        "//pkg/kubelet/config:go_default_library",
        "//pkg/kubelet/status:go_default_library",
        "//staging/src/k8s.io/api/core/v1:go_default_library",
        "//staging/src/k8s.io/cri-api/pkg/apis/runtime/v1alpha2:go_default_library",
        "//vendor/github.com/google/cadvisor/info/v1:go_default_library",
    ],
)

filegroup(
    name = "package-srcs",
    srcs = glob(["**"]),
    tags = ["automanaged"],
    visibility = ["//visibility:private"],
)

filegroup(
    name = "all-srcs",
    srcs = [
        ":package-srcs",
        "//pkg/kubelet/cm/memorymanager/state:all-srcs",
    ],
    tags = ["automanaged"],
    visibility = ["//visibility:public"],
)
