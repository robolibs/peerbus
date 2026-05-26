{
  description = "bevy_mara — reusable glass-themed Bevy + egui editor UI kit, development shell";

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
    { self, nixpkgs, rust-overlay, flake-utils, nixgl, ... }:
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

        nvidiaVersion = let v = builtins.getEnv "NVIDIA_VERSION";
        in if v != "" then v
           else throw "bevy_mara: NVIDIA_VERSION is unset — is direnv loaded and is the NVIDIA driver running?";

        nixglPkgs = import "${nixgl}/default.nix" {
          inherit pkgs nvidiaVersion;
          nvidiaHash = null;
        };

        nixGLAlias = pkgs.runCommand "nixGL" { } ''
          mkdir -p $out/bin
          ln -s ${nixglPkgs.nixGLNvidia}/bin/nixGLNvidia-${nvidiaVersion} $out/bin/nixGL
        '';
        nixVulkanAlias = pkgs.runCommand "nixVulkan" { } ''
          mkdir -p $out/bin
          ln -s ${nixglPkgs.nixVulkanNvidia}/bin/nixVulkanNvidia-${nvidiaVersion} $out/bin/nixVulkan
        '';

        bevyLibs = with pkgs; [
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

        # iceoryx2 has C bindings; its build.rs runs bindgen which
        # needs libclang on LIBCLANG_PATH and libstdc++ resolvable
        # via LD_LIBRARY_PATH. Without these, bindgen falls back to
        # /usr/lib/llvm-* and fails on libstdc++.so.6.
        nativeBuildLibs = with pkgs; [
          libclang.lib
          stdenv.cc.cc.lib
        ];
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

            pkgs.trunk

            (pkgs.python3.withPackages (ps: with ps; [ fonttools brotli ]))

            nixGLAlias
            nixVulkanAlias
            nixglPkgs.nixGLNvidia
            nixglPkgs.nixVulkanNvidia
            nixglPkgs.nixGLIntel
            nixglPkgs.nixVulkanIntel
          ] ++ bevyLibs ++ nativeBuildLibs;

          RUST_SRC_PATH = "${pkgs.rust.packages.stable.rustPlatform.rustLibSrc}";
          LIBCLANG_PATH = "${pkgs.libclang.lib}/lib";
          LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath (bevyLibs ++ nativeBuildLibs);
          WGPU_VALIDATION = "0";
          WGPU_DEBUG = "0";
        };
      }
    );
}
