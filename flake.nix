{
  description = "robolibs crate development shell";

  inputs = {
    # Pinned to a rev that still accepts the `kernel` arg in
    # nvidia-x11/generic.nix. Newer nixpkgs (post 2026-04) dropped
    # that arg, which breaks nixGL until upstream catches up. Bump
    # together with nixgl when its corresponding fix lands.
    nixpkgs.url = "github:NixOS/nixpkgs?rev=4c1018dae018162ec878d42fec712642d214fdfa";
    rust-overlay.url = "github:oxalica/rust-overlay?rev=3c27f4c92a7d977556dd2c10bb564d9c61b375e9";
    flake-utils.url = "github:numtide/flake-utils";
    nixgl.url = "github:nix-community/nixGL";
  };

  outputs =
    { nixpkgs, rust-overlay, flake-utils, nixgl, ... }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        overlays = [
          (final: prev: {
            xorg = prev.xorg // {
              libX11 = final.libx11;
              libxcb = final.libxcb;
              libxshmfence = final.libxshmfence;
            };
          })
          (import rust-overlay)
        ];

        pkgs = import nixpkgs {
          inherit system overlays;
          config = {
            allowUnfree = true;
            nvidia.acceptLicense = true;
          };
        };

        nvidiaVersion = builtins.getEnv "NVIDIA_VERSION";
        hasNvidia = nvidiaVersion != "";

        nixglPkgs = import "${nixgl}/default.nix" ({
          inherit pkgs;
        } // pkgs.lib.optionalAttrs hasNvidia {
          inherit nvidiaVersion;
          nvidiaHash = null;
        });

        nixGLTarget =
          if hasNvidia
          then "${nixglPkgs.nixGLNvidia}/bin/nixGLNvidia-${nvidiaVersion}"
          else "${nixglPkgs.nixGLIntel}/bin/nixGLIntel";
        nixVulkanTarget =
          if hasNvidia
          then "${nixglPkgs.nixVulkanNvidia}/bin/nixVulkanNvidia-${nvidiaVersion}"
          else "${nixglPkgs.nixVulkanIntel}/bin/nixVulkanIntel";

        nixGLAlias = pkgs.runCommand "nixGL" { } ''
          mkdir -p $out/bin
          ln -s ${nixGLTarget} $out/bin/nixGL
        '';
        nixVulkanAlias = pkgs.runCommand "nixVulkan" { } ''
          mkdir -p $out/bin
          ln -s ${nixVulkanTarget} $out/bin/nixVulkan
        '';

        guiLibs = with pkgs; [
          alsa-lib
          udev
          vulkan-loader
          libxkbcommon
          wayland
          libx11
          libxcursor
          libxi
          libxrandr
        ];

        pythonEnv = pkgs.python3.withPackages (ps: with ps; [
          brotli
          fonttools
          pip
        ]);

        quicbitPythonDevelop = pkgs.writeShellScriptBin "quicbit-python-develop" ''
          set -euo pipefail

          root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
          if [ ! -d "$root/examples/python_binding" ]; then
            echo "quicbit-python-develop: run from the quicbit repository" >&2
            exit 2
          fi
          if [ ! -d "$root/../datapod" ]; then
            echo "quicbit-python-develop: expected ../datapod next to quicbit" >&2
            exit 2
          fi

          export VIRTUAL_ENV="''${VIRTUAL_ENV:-$root/.nix-python}"
          export PATH="$VIRTUAL_ENV/bin:$PATH"
          export PYO3_PYTHON="''${PYO3_PYTHON:-$VIRTUAL_ENV/bin/python}"
          export PYTHON="''${PYTHON:-$VIRTUAL_ENV/bin/python}"

          (cd "$root/../datapod" && maturin develop --features python)
          (cd "$root" && maturin develop --features python)
        '';

        quicbitVideoPub = pkgs.writeShellScriptBin "quicbit-video-pub" ''
          set -euo pipefail

          root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
          cd "$root"
          exec python examples/python_binding/video_pub.py "$@"
        '';

        quicbitVideoSub = pkgs.writeShellScriptBin "quicbit-video-sub" ''
          set -euo pipefail

          root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
          cd "$root"
          exec python examples/python_binding/video_sub.py "$@"
        '';
      in
      {
        devShells.default = pkgs.mkShell {
          packages = [
            (pkgs.rust-bin.stable.latest.default.override {
              extensions = [ "rust-src" "rustfmt" "clippy" ];
              targets = [ "wasm32-unknown-unknown" ];
            })
            pkgs.clang
            pkgs.mold
            pkgs.pkg-config
            pkgs.rust-cbindgen
            pkgs.trunk
            pkgs.maturin
            pkgs.git
            pythonEnv
            quicbitPythonDevelop
            quicbitVideoPub
            quicbitVideoSub

            nixGLAlias
            nixVulkanAlias
            nixglPkgs.nixGLIntel
            nixglPkgs.nixVulkanIntel
          ] ++ pkgs.lib.optionals hasNvidia [
            nixglPkgs.nixGLNvidia
            nixglPkgs.nixVulkanNvidia
          ] ++ guiLibs;

          RUST_SRC_PATH = "${pkgs.rust.packages.stable.rustPlatform.rustLibSrc}";
          LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath guiLibs;
          WGPU_VALIDATION = "0";
          WGPU_DEBUG = "0";

          shellHook = ''
            export QUICBIT_PY_VENV="$PWD/.nix-python"
            if [ ! -x "$QUICBIT_PY_VENV/bin/python" ]; then
              ${pythonEnv}/bin/python -m venv --system-site-packages "$QUICBIT_PY_VENV"
            fi
            export VIRTUAL_ENV="$QUICBIT_PY_VENV"
            export PATH="$VIRTUAL_ENV/bin:$PATH"
            export PYO3_PYTHON="$VIRTUAL_ENV/bin/python"
            export PYTHON="$VIRTUAL_ENV/bin/python"

            quicbit-python-ready() {
              python - <<'PY' >/dev/null 2>&1
import inspect
import datapod
import quicbit
sig = str(inspect.signature(quicbit.Node))
assert "max_publishers" in sig and "max_subscribers" in sig, sig
PY
            }

            if ! quicbit-python-ready; then
              echo "Installing local datapod/quicbit Python bindings into $VIRTUAL_ENV ..."
              quicbit-python-develop
            fi

            echo "Python: $(python --version) ($PYO3_PYTHON)"
            echo "Refresh bindings after code changes: quicbit-python-develop"
            echo "Python video pub: quicbit-video-pub"
            echo "Python video sub: quicbit-video-sub <did:key:...>"
          '';
        };
      }
    );
}
