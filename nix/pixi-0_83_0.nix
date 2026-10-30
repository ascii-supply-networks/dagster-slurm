{
  stdenvNoCC,
  fetchurl,
}:
stdenvNoCC.mkDerivation rec {
  pname = "pixi";
  version = "0.83.0";

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
        aarch64-darwin = "sha256-U0bMNGW2NMGpAfvlGy7L9qZEQgMW6UGsDuYDLF5IwZE=";
        x86_64-darwin = "sha256-s3jbc6wrKjXeUv5O6IharqWBLQsjKquRVK9NAGM58SI=";
        aarch64-linux = "sha256-X7y7P+/ylNpNKF1y7+t261mP2PyUodkoPUTGLaudZAs=";
        x86_64-linux = "sha256-UFcUUelvSoLPZ1zYO6r2b/+bcY8KrxU5ALn69MKb1AE=";
      }
      .${stdenvNoCC.hostPlatform.system}
        or (throw "Unsupported Pixi platform: ${stdenvNoCC.hostPlatform.system}");
  };

  dontUnpack = true;

  installPhase = ''
    install -Dm755 "$src" "$out/bin/pixi"
  '';
}
