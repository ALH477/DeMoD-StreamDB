{
  description = "StreamDB - Deterministic Docker Build";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        manifest = (pkgs.lib.importTOML ./Cargo.toml).package;

        streamdb = pkgs.rustPlatform.buildRustPackage {
          pname = manifest.name;
          version = manifest.version;
          src = ./.;
          
          cargoLock.lockFile = ./Cargo.lock;
          buildFeatures = [ "ffi" ];
          doCheck = false;

          # Adjusted to ensure it finds the header regardless of subfolder
          postInstall = ''
            mkdir -p $out/lib $out/include
            find . -name "streamdb.h" -exec cp {} $out/include/ \;
            find target -name "libstreamdb.so" -exec cp {} $out/lib/ \;
          '';
        };

        dockerImage = pkgs.dockerTools.buildLayeredImage {
          name = "streamdb-runtime";
          tag = "latest";
          created = "now";
          contents = [ 
            streamdb 
            pkgs.bashInteractive 
            pkgs.coreutils 
            pkgs.glibc 
          ];

          config = {
            Env = [ "LD_LIBRARY_PATH=/lib" ];
            Cmd = [ 
              "${pkgs.bashInteractive}/bin/bash" 
              "-c" 
              "ls -la /lib/libstreamdb.so /include/streamdb.h" 
            ];
          };
        };
      in
      {
        packages = {
          default = streamdb;
          docker = dockerImage;
        };
      }
    );
}
