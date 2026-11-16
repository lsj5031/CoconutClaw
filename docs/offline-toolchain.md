# Offline Linux development linker shim

The acceptance environment already provides Rust, cached Cargo dependencies,
Emscripten's native clang/llvm-ar, glibc startup objects and Rust's bundled lld.
The inherited shim under `/tmp/coconutclaw-toolchain` is an environment workaround,
not a product dependency or a cross-platform release toolchain. Do not overwrite
that provided directory or install/download anything to regenerate it.

To reconstruct the same shim in a **new, unused directory**, using those exact
already provisioned offline inputs:

```sh
shim=/chosen/new/offline-linker
test ! -e "$shim" || exit 1
mkdir -m 700 "$shim"
export SHIM="$shim"
python3 - <<'PY'
import os, pathlib
p = pathlib.Path(os.environ["SHIM"])
p.joinpath("cc").write_text("""#!/bin/sh
tools=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd) || exit 1
shared=false
for arg do
    case "$arg" in -shared) shared=true;; esac
done
if "$shared"; then
    exec /home/leo/.cache/emsdk/upstream/bin/clang "-fuse-ld=$tools/ld.lld" -nostartfiles "-L$tools" "$@"
fi
exec /home/leo/.cache/emsdk/upstream/bin/clang "-fuse-ld=$tools/ld.lld" -nostartfiles /usr/lib/x86_64-linux-gnu/Scrt1.o /usr/lib/x86_64-linux-gnu/crti.o /usr/lib/x86_64-linux-gnu/crtn.o "-L$tools" "$@"
""")
p.joinpath("cc").chmod(0o700)
p.joinpath("ld.lld").symlink_to("/home/leo/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/lib/rustlib/x86_64-unknown-linux-gnu/bin/rust-lld")
for name in ("libgcc.so", "libgcc_s.so"):
    p.joinpath(name).symlink_to("/usr/lib/x86_64-linux-gnu/libgcc_s.so.1")
PY
CC="$shim/cc" AR=/home/leo/.cache/emsdk/upstream/bin/llvm-ar \
 CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER="$shim/cc" \
 cargo build --workspace --locked --offline
```

Paths intentionally describe this Linux environment, not every host. This
recreates only the wrapper/symlinks, not compiler/linker binaries. If an input is
absent, provision that input through the authorized offline environment first.
Native macOS/Windows use their normal native toolchains.

The checkpoint retains wrapper/input hashes and symlink targets alongside its
exact command receipts. The original wrapper SHA256 is
`7d2d36d0e732128b677e23ecd5551a40a8cedc4e48964b66838b4885aa9f4240`;
llvm-ar is
`548f3d9e46b8d0bac1ec79b4e74e7b9e48325444ac34636f4277b8c701e3dd98`.
No global Cargo, authentication or private memory configuration is changed.
