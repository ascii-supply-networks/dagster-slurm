{
  stdenvNoCC,
  fetchurl,
  unzip,
}:
stdenvNoCC.mkDerivation rec {
  pname = "bun";
  version = "1.4.4";

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
        aarch64-darwin = "sha256-wkDX8My5DCY6hJ/NsJt+5iYw2SE0jtaq/ahrnVd7xRA=";
        x86_64-darwin = "sha256-UKNJH3Orwv6BZzFxqFdi07wswWgMzsrdmD3pmt93l34=";
        aarch64-linux = "sha256-uqa7NvUdblhWHgK76oJQOKnfRU9nPW13bou/hvjHhQ4=";
        x86_64-linux = "sha256-v4zL+ysLgBWeD5ptFcVTF63YB3U028YTCj7bUvzLOVw=";
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
