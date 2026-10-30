{
  inputs = {
    nix-jettison.url = "git+ssh://git@github.com/noib3/nix-jettison.git";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    treefmt-nix = {
      url = "github:numtide/treefmt-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nix-jettison,
      nixpkgs,
      rust-overlay,
      treefmt-nix,
      ...
    }:
    let
      forEachSystem =
        f:
        nixpkgs.lib.genAttrs [
          "aarch64-darwin"
          "aarch64-linux"
          "x86_64-linux"
        ] (system: f system nixpkgs.legacyPackages.${system});

      mkToolchain =
        pkgs:
        ((rust-overlay.lib.mkRustBin { } pkgs.buildPackages).fromRustupToolchainFile
          ./rust-toolchain.toml
        ).override
          {
            targets = [ pkgs.stdenv.targetPlatform.rust.rustcTarget ];
          };

      mkBuildInputs =
        pkgs:
        let
          # HEAD of https://github.com/NixOS/nix/pull/15675
          nixSource = pkgs.fetchFromGitHub {
            owner = "NixOS";
            repo = "nix";
            rev = "9d718847a1b97cc8476cb23c207cacf91ba136ed";
            sha256 = "11cnvlsfv1wimhljdb5483kfd3gg3vijixf38lzssh4l3c3h7z2l";
          };

          components =
            (
              (pkgs.pkgsStatic.nixDependencies.callPackage
                "${nixpkgs}/pkgs/tools/package-management/nix/modular/packages.nix"
                {
                  src = nixSource;
                  version = "2.35.0";
                  otherSplices = pkgs.pkgsStatic.generateSplicesForMkScope [
                    "nixVersions"
                    "nixComponents_git"
                  ];
                  teams = [ ];
                }
              ).appendPatches
              [
                ./nix/patches/nix-store-optional-curl.patch
                # Keep the C API pkg-config files from exposing internal C++
                # libraries as public link dependencies.
                ./nix/patches/nix-libutil-c-pkg-config-private-deps.patch
                ./nix/patches/nix-libstore-c-pkg-config-private-deps.patch
                # Fix static pkg-config metadata so downstream consumers link
                # Boost and the C++ runtime correctly.
                ./nix/patches/nix-libutil-pkg-config-static-libs.patch
                ./nix/patches/nix-libstore-pkg-config-static-libs.patch
              ]
            ).overrideScope
              (
                final: prev: {
                  nix-util = (prev.nix-util).overrideAttrs (old: {
                    propagatedBuildInputs = pkgs.lib.filter (
                      pkg: (pkg.pname or "") != "libarchive"
                    ) old.propagatedBuildInputs;
                    mesonFlags = old.mesonFlags ++ [
                      (pkgs.lib.mesonBool "archive-support" false)
                    ];
                  });
                  nix-util-c = (
                    prev.nix-util-c.override {
                      nix-util = final.nix-util;
                    }
                  );
                  nix-store =
                    (prev.nix-store.override {
                      nix-util = final.nix-util;
                      # nixpkgs currently enables the embedded sandbox shell for
                      # all static builds, but only wires the busybox sandbox
                      # shell path on Linux. Disable the embedded shell on
                      # non-Linux targets so the static package still builds.
                      embeddedSandboxShell =
                        pkgs.stdenv.hostPlatform.isLinux && pkgs.stdenv.hostPlatform.isStatic;
                      # We don't need authenticated S3 fetcher support, so let's
                      # disable it to make the release binary smaller.
                      withAWS = false;
                    }).overrideAttrs
                      (old: {
                        propagatedBuildInputs = pkgs.lib.filter (
                          pkg: (pkg.pname or "") != "curl"
                        ) old.propagatedBuildInputs;
                        buildInputs = pkgs.lib.remove prev.curl old.buildInputs;
                        mesonFlags = old.mesonFlags ++ [
                          (pkgs.lib.mesonBool "http-client" false)
                          (pkgs.lib.mesonBool "extra-store-implementations" false)
                        ];
                      });
                  nix-store-c = (
                    prev.nix-store-c.override {
                      nix-util-c = final.nix-util-c;
                      nix-store = final.nix-store;
                    }
                  );
                }
              );
        in
        [
          components.nix-util.dev
          components.nix-util-c.dev
          components.nix-store.dev
          components.nix-store-c.dev
        ];

      # Darwin's wrapped toolchain still injects the shared libiconv search
      # path via NIX_LDFLAGS. Rewrite those flags so `-liconv` resolves to the
      # static archive in both the dev shell and packaged builds.
      mkStaticLinkEnvHook =
        pkgs:
        pkgs.lib.optionalString pkgs.stdenv.hostPlatform.isDarwin ''
          rewrite_iconv_flags() {
            local var_name=$1
            local value="''${!var_name:-}"
            local rewritten=

            for flag in $value; do
              case "$flag" in
                -liconv)
                  flag='${pkgs.pkgsStatic.libiconv.dev}/lib/libiconv.a'
                  ;;
                -L*-libiconv-*)
                  case "$flag" in
                    *-static-*) ;;
                    *) continue ;;
                  esac
                  ;;
              esac

              rewritten="$rewritten $flag"
            done

            printf -v "$var_name" '%s' "''${rewritten# }"
            export "$var_name"
          }

          rewrite_iconv_flags NIX_LDFLAGS
          rewrite_iconv_flags NIX_LDFLAGS_FOR_BUILD
        '';

      mkWorkspace =
        pkgs:
        import ./nix/workspace.nix {
          inherit pkgs self nix-jettison;
          nixLibraries = mkBuildInputs pkgs;
          toolchain = mkToolchain pkgs;
        };

      mkPackage =
        pkgs:
        (mkWorkspace pkgs).rootPackage.build.override {
          binaries = true;
          features = [ "sub60-cache" ];
        };

      mkTreefmt =
        pkgs:
        treefmt-nix.lib.evalModule pkgs {
          projectRootFile = "flake.nix";

          programs.nixfmt = {
            enable = true;
            width = 80;
          };

          programs.prettier.enable = true;

          programs.rustfmt = {
            enable = true;
            package = mkToolchain pkgs;
          };

          settings.formatter.prettier.options = [
            "--config"
            "${./prettier.config.json}"
          ];

          settings.formatter.rustfmt.options = [
            "--config-path"
            "${./rustfmt.toml}"
          ];
        };
    in
    {
      packages = forEachSystem (
        _system: pkgs:
        {
          default = mkPackage pkgs;
          aarch64-linux = mkPackage pkgs.pkgsCross.aarch64-multiplatform-musl;
          x86_64-linux = mkPackage pkgs.pkgsCross.musl64;
        }
        // nixpkgs.lib.optionalAttrs (pkgs.stdenv.buildPlatform.isDarwin) {
          aarch64-darwin = mkPackage pkgs.pkgsCross.aarch64-darwin;
          x86_64-darwin = mkPackage pkgs.pkgsCross.x86_64-darwin;
        }
      );

      # Workaround for https://github.com/NixOS/nix/issues/8881 so that we
      # can run individual checks with `nix run .#check-<foo>`.
      apps = forEachSystem (
        system: pkgs:
        nixpkgs.lib.mapAttrs' (name: check: {
          name = "check-${name}";
          value = {
            type = "app";
            program =
              (pkgs.writeShellScript "check-${name}" ''
                # Force evaluation of ${check}.
                echo -e "\033[1;32m✓\033[0m Check '${name}' passed"
              '').outPath;
          };
        }) self.checks.${system}
      );

      devShells = forEachSystem (
        _system: pkgs: {
          default = pkgs.mkShell {
            buildInputs = mkBuildInputs pkgs;
            nativeBuildInputs = [
              ((mkToolchain pkgs).override {
                extensions = [
                  "clippy"
                  "rust-analyzer"
                  "rust-src"
                  "rustfmt"
                ];
              })
              pkgs.buildPackages.pkg-config
              (mkTreefmt pkgs).config.build.programs.prettier
              pkgs.nodejs
              pkgs.typescript-language-server
            ];
            shellHook = mkStaticLinkEnvHook pkgs;
            env.PKG_CONFIG_ALL_STATIC = "1";
          };
        }
      );

      formatter = forEachSystem (
        _system: pkgs: (mkTreefmt pkgs).config.build.wrapper
      );

      checks = forEachSystem (
        _system: pkgs: {
          clippy = (mkWorkspace pkgs).clippy.override {
            allFeatures = true;
            allTargets = true;
            clippyArgs = [
              "-D"
              "warnings"
            ];
          };
          formatting = (mkTreefmt pkgs).config.build.check self;
        }
      );
    };

  nixConfig = {
    extra-substituters = [
      "https://cache.sub60.dev/sub60/setup-cache"
      "https://cache.sub60.dev/noib3/nix-jettison"
    ];
    extra-trusted-public-keys = [
      "cache.sub60.dev/sub60/setup-cache-1:eLhgNfgMDRVlQ9EpHGWujSfjLZg21EGTd+lDCOuV0jY="
      "cache.sub60.dev/noib3/nix-jettison-1:yFjuL2ZV5kv1FDUlU76T6yEh6/i7Ud3qF+QxgdZt0Ko="
    ];
  };
}
