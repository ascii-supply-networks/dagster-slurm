{
  stdenvNoCC,
  fetchurl,
  unzip,
}:
stdenvNoCC.mkDerivation rec {
  pname = "bun";
  version = "1.3.14";

  platform =
    {
      aarch64-darwin = "bun-darwin-aarch64";
      x86_64-darwin = "bun-darwin-x64";
      aarch64-linux = "bun-linux-aarch64";
      x86_64-linux = "bun-linux-x64";
    }
    .${stdenvNoCC.hostPlatform.system}
      or (throw "Unsupported Bun platform: ${stdenvNoCC.hostPlatform.system}");

  src = fetchurl {
    url = "https://github.com/oven-sh/bun/releases/download/bun-v${version}/${platform}.zip";
    hash =
      {
        aarch64-darwin = "sha256-2LliIYKK1vl6x6wKt+lYcjQa92MAHogD6CZ2UsJlJiA=";
        x86_64-darwin = "sha256-QYPfM3RiPlurMVxUfPoJdFM81FfYa3O2OfeoeXTNZjM=";
        aarch64-linux = "sha256-on/7Y6gxA3WDbg1vZorhf6jY0YuIw3yCHGUzGXOhmjs=";
        x86_64-linux = "sha256-lR7iruhV8IWVruxiJSJqKY0/6oOj3NZGXAnLzN9+hI8=";
      }
      .${stdenvNoCC.hostPlatform.system}
        or (throw "Unsupported Bun platform: ${stdenvNoCC.hostPlatform.system}");
  };

  nativeBuildInputs = [unzip];

  installPhase = ''
    install -Dm755 bun "$out/bin/bun"
    ln -s bun "$out/bin/bunx"
  '';
}
