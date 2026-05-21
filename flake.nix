{
  description = "quicbit Rust library development shell";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    { nixpkgs, rust-overlay, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        overlays = [ (import rust-overlay) ];

        pkgs = import nixpkgs {
          inherit system overlays;
        };

        # Stable: default day-to-day toolchain.
        stableToolchain = pkgs.rust-bin.stable.latest.default.override {
          extensions = [ "rust-src" "rustfmt" "clippy" ];
        };

        # Nightly: for miri (unsafe-code auditor).
        # Only miri requires nightly; everything else
        # (build, test, clippy, fmt) lives on stable.
        nightlyToolchain = pkgs.rust-bin.selectLatestNightlyWith (
          toolchain:
          toolchain.default.override {
            extensions = [ "rust-src" "miri" "rustfmt" "clippy" ];
          }
        );

        common = [
          pkgs.clang
          pkgs.mold
          pkgs.pkg-config
          # iceoryx2 has C bindings; its build.rs runs bindgen,
          # which needs libclang + the C++ runtime resolvable at
          # link time.
          pkgs.libclang.lib
          pkgs.stdenv.cc.cc.lib
        ];

        # Env vars exported into every devShell so bindgen finds
        # libclang and the dynamic linker finds libstdc++.
        shellEnv = {
          LIBCLANG_PATH = "${pkgs.libclang.lib}/lib";
          LD_LIBRARY_PATH = nixpkgs.lib.makeLibraryPath [
            pkgs.stdenv.cc.cc.lib
            pkgs.libclang.lib
          ];
        };
      in
      {
        devShells = {
          # `nix develop`  →  stable shell, used for all normal work.
          default = pkgs.mkShell ({
            packages = [ stableToolchain ] ++ common;
            RUST_SRC_PATH = "${pkgs.rust.packages.stable.rustPlatform.rustLibSrc}";
          } // shellEnv);

          # `nix develop .#nightly`  →  adds miri.
          # Use for:
          #   cargo miri test --lib
          nightly = pkgs.mkShell ({
            packages = [
              nightlyToolchain
            ] ++ common;
          } // shellEnv);

          # `nix develop .#python`  →  adds Python + maturin so the
          # `python` feature links and Python tests run.
          # Build a wheel with:
          #   maturin build --release --features python-extension
          python = pkgs.mkShell ({
            packages = [
              stableToolchain
              pkgs.python312
              pkgs.python312Packages.pip
              pkgs.maturin
            ] ++ common;
            RUST_SRC_PATH = "${pkgs.rust.packages.stable.rustPlatform.rustLibSrc}";
          } // shellEnv);
        };
      }
    );
}
