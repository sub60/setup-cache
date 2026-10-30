{
  pkgs,
  # --
  self,
  nix-jettison,
  nixLibraries,
  staticLinkEnvHook,
  toolchain,
}:

let
  # Lets the crate's build script find Nix's static libraries via pkg-config.
  withNixLibraries =
    pkgs': args:
    args
    // {
      nativeBuildInputs = (args.nativeBuildInputs or [ ]) ++ [
        pkgs'.buildPackages.pkg-config
      ];
      buildInputs = (args.buildInputs or [ ]) ++ nixLibraries;
      env = args.env // {
        PKG_CONFIG_ALL_STATIC = "1";
      };
    };
in
nix-jettison.lib.loadWorkspace {
  inherit pkgs toolchain;
  src = self;
  crateOverrides = pkgs': {
    cache-action =
      args:
      withNixLibraries pkgs' args
      // {
        preBuild = (args.preBuild or "") + staticLinkEnvHook;
      };
    nixb-cpp = withNixLibraries pkgs';
  };
}
