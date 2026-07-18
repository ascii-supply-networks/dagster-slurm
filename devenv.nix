{
  inputs,
  pkgs,
  ...
}: let
  staxCargoToml = builtins.fromTOML (builtins.readFile "${inputs.stax-src}/Cargo.toml");
  staxPackageVersion = staxCargoToml.package.version;
  staxVersion =
    if builtins.isAttrs staxPackageVersion && (staxPackageVersion.workspace or false)
    then staxCargoToml.workspace.package.version
    else staxPackageVersion;
  pixi = pkgs.callPackage ./nix/pixi-0_73_0.nix {};
  bun = pkgs.callPackage ./nix/bun-1_3_14.nix {};
in {
  languages.python = {
    enable = true;
    package = pkgs.python312;
  };

  dotenv.enable = true;

  env = {
    UV_PYTHON_PREFERENCE = "only-system";
    UV_PREVIEW = "1";
  };

  overlays = [
    (final: prev: {
      stax = final.rustPlatform.buildRustPackage {
        pname = "stax";
        version = staxVersion;
        src = inputs.stax-src;

        cargoLock.lockFile = "${inputs.stax-src}/Cargo.lock";

        nativeBuildInputs = [
          final.perl
          final.pkg-config
        ];
        buildInputs = [final.openssl];

        doCheck = false;
        doInstallCheck = true;
        nativeInstallCheckInputs = [final.versionCheckHook];
        versionCheckProgramArg = "--version";

        meta = {
          description = "Stacked-branch workflow for Git with an interactive TUI, smart PRs, and safe undo";
          homepage = "https://github.com/cesarferreira/stax";
          license = final.lib.licenses.mit;
          mainProgram = "stax";
        };
      };
    })
  ];

  packages = with pkgs; [
    bun
    dprint
    fd
    gh
    git
    just
    pixi
    ripgrep
    stax
    uv
  ];

  enterShell = ''
    echo "dagster-slurm development environment"
    echo "Python: $(python --version)"
    echo "Pixi: $(pixi --version)"
    echo "Bun: $(bun --version)"
    echo "uv: $(uv --version)"
    echo "Just: $(just --version)"
    echo "Stax: $(stax --version)"
    echo "Dprint: $(dprint --version)"
    echo "rg: $(rg --version | head -n 1)"
  '';

  enterTest = ''
    python --version
    pixi --version
    bun --version
    uv --version
    just --version
    stax --version
    dprint --version
    rg --version
    fd --version
    gh --version
  '';
}
