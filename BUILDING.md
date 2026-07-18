# Building Flywheel

Flywheel builds on Linux and macOS with the stable Rust toolchain. RocksDB bindings require a C++
toolchain and libclang at build time.

## Prerequisites

- Stable Rust with Rust 2024 edition support
- GNU Make
- C and C++ compiler toolchain
- Clang and libclang
- CMake
- pkg-config

Ubuntu and Debian:

```text
sudo apt-get update
sudo apt-get install --yes build-essential clang cmake libclang-dev pkg-config
```

macOS with Homebrew:

```text
brew install llvm cmake pkg-config
```

The Makefile discovers Homebrew's LLVM prefix and exports `LIBCLANG_PATH` automatically. For a
non-Homebrew LLVM installation, set it explicitly:

```text
export LIBCLANG_PATH=/path/to/llvm/lib
```

## Build

Clone the repository and build all features:

```text
git clone https://github.com/ctxswitch/flywheel.git
cd flywheel
make build
```

The debug binary is written to `target/debug/flywheel`. Build the optimized binary from the
locked dependency graph with:

```text
make release
```

The release binary is written to `target/release/flywheel`.

## Run locally

```text
mkdir -p /tmp/flywheel-data
./target/debug/flywheel serve --data-dir /tmp/flywheel-data
curl --fail http://127.0.0.1:8080/health/ready
```

The data directory contains the embedded metadata store and cached artifacts. Reuse it to test
restart behavior; remove it only when a fresh store is required.

## Quality checks

Run the same code gates used by pull requests:

```text
make ci
```

Individual targets are available while iterating:

| Command | Purpose |
| --- | --- |
| `make fmt` | Format Rust sources |
| `make fmt-check` | Check formatting without changing files |
| `make lint` | Run Clippy with warnings denied |
| `make test` | Run the test suite |
| `make check` | Type-check all targets |
| `make release` | Produce an optimized binary |

Integration tests bind loopback ports and use temporary directories. They do not require a
running Flywheel process or public network access.

## Container image and Helm chart

The container build is the production packaging path, not a prerequisite for local development:

```text
docker build -t flywheel:dev .
```

Validate chart changes separately:

```text
helm lint charts/flywheel --strict
```

See [charts/flywheel/README.md](charts/flywheel/README.md) for deployment values and
[docs/operations.md](docs/operations.md) for service operation.

## Troubleshooting

### libclang cannot be found

Errors from `clang-sys` or `bindgen` that mention `libclang.so`, `libclang.dylib`, or
`llvm-config` mean the shared libclang installation is missing or undiscoverable. Install the
platform package above and point `LIBCLANG_PATH` at the directory containing the shared library.

### Linker or C++ build failures

Confirm that the C++ compiler and CMake are installed, then remove partial native build output and
retry:

```text
cargo clean
make build
```

### Disk usage

Rust and RocksDB compilation produce a large `target/` directory. `make clean` removes only build
output; it does not remove a Flywheel data directory supplied to `serve`.
