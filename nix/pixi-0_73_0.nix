{
  stdenvNoCC,
  fetchurl,
}:
stdenvNoCC.mkDerivation rec {
  pname = "pixi";
  version = "0.73.0";

  platform =
    {
      aarch64-darwin = "aarch64-apple-darwin";
      x86_64-darwin = "x86_64-apple-darwin";
      aarch64-linux = "aarch64-unknown-linux-musl";
      x86_64-linux = "x86_64-unknown-linux-musl";
    }
    .${stdenvNoCC.hostPlatform.system}
      or (throw "Unsupported Pixi platform: ${stdenvNoCC.hostPlatform.system}");

  src = fetchurl {
    url = "https://github.com/prefix-dev/pixi/releases/download/v${version}/pixi-${platform}";
    hash =
      {
        aarch64-darwin = "sha256-Y/M1Bg0L2ivGfKSHr75GD8IP/Sjo6LSHiEWiBquXLIY=";
        x86_64-darwin = "sha256-KAB29x8Y50kgZHE6d55UfqDHM3bI8U3DoLSagPN40dI=";
        aarch64-linux = "sha256-U54ixHKR//XpML/DZUtail7oCefsGp91aD2GYL3NQZE=";
        x86_64-linux = "sha256-cSejk9oR/3x2sfvEWHMeJKuBBcPdtBVFnNhf2Ep15xU=";
      }
      .${stdenvNoCC.hostPlatform.system}
        or (throw "Unsupported Pixi platform: ${stdenvNoCC.hostPlatform.system}");
  };

  dontUnpack = true;

  installPhase = ''
    install -Dm755 "$src" "$out/bin/pixi"
  '';
}
