{
  pkgs,
  # --
  self,
  nix-jettison,
  nixLibraries,
  toolchain,
}:

let
  # Lets the crate's build script find Nix's static libraries via pkg-config.
  withNixLibraries =
    pkgs': args:
    args
    // {
      buildScript = args.buildScript // {
        nativeBuildInputs = [ pkgs'.pkg-config ];
        buildInputs = nixLibraries;
        env.PKG_CONFIG_ALL_STATIC = "1";
      };
    };
in
nix-jettison.lib.loadWorkspace {
  inherit pkgs toolchain;
  src = self;
  crateOverrides = pkgs': {
    cache-action = withNixLibraries pkgs';
    nixb-cpp = withNixLibraries pkgs';
  };
}
