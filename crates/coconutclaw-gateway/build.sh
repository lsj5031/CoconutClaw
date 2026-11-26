#!/bin/sh
set -eu
cd "$(dirname "$0")"
cargo build --locked --release --target wasm32-unknown-unknown --target-dir build/target
rm -rf build/worker
wasm-bindgen --target web --no-typescript --out-dir build/worker \
    build/target/wasm32-unknown-unknown/release/coconutclaw_gateway.wasm
