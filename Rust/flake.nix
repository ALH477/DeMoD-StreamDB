{
  description = "StreamDB - Deterministic AI-Ready Container";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        manifest = (pkgs.lib.importTOML ./Cargo.toml).package;

        # 1. The Core Rust Library
        streamdb = pkgs.rustPlatform.buildRustPackage {
          pname = manifest.name;
          version = manifest.version;
          src = ./.;
          cargoLock.lockFile = ./Cargo.lock;
          buildFeatures = [ "ffi" ];
          doCheck = false;

          # Install artifacts to Nix store paths
          postInstall = ''
            mkdir -p $out/lib $out/include
            
            # Correctly copy the header from the include directory
            cp include/streamdb.h $out/include/streamdb.h
            
            # Find and move the shared object
            find target -name "libstreamdb.so" -exec cp {} $out/lib/ \;
          '';
        };

        # 2. Production-Ready Docker Image
        dockerImage = pkgs.dockerTools.buildLayeredImage {
          name = "streamdb-ai-ready";
          tag = "latest";
          created = "now";
          
          contents = with pkgs; [ 
            streamdb 
            python311
            bashInteractive 
            coreutils 
            glibc 
            cacert 
          ];

          # FIX: Use 'ln -sf' (force) to prevent "File exists" errors
          extraCommands = ''
            mkdir -p bin usr/bin lib include app tmp

            # 1. Link Core Utilities
            ln -sf ${pkgs.coreutils}/bin/ls bin/ls
            ln -sf ${pkgs.coreutils}/bin/mkdir bin/mkdir
            ln -sf ${pkgs.coreutils}/bin/cp bin/cp
            ln -sf ${pkgs.coreutils}/bin/cat bin/cat
            
            # 2. Link Shells
            ln -sf ${pkgs.bashInteractive}/bin/bash bin/bash
            ln -sf ${pkgs.bashInteractive}/bin/sh bin/sh

            # 3. Link Python
            ln -sf ${pkgs.python311}/bin/python3 bin/python3
            ln -sf ${pkgs.python311}/bin/python3 usr/bin/python3

            # 4. Link StreamDB
            ln -sf ${streamdb}/lib/libstreamdb.so lib/libstreamdb.so
            ln -sf ${streamdb}/include/streamdb.h include/streamdb.h
          '';

          config = {
            Env = [ 
              "PATH=/bin:/usr/bin" 
              "LD_LIBRARY_PATH=/lib" 
              "PYTHONPATH=/app"
              "STREAMDB_HEADER_PATH=/include/streamdb.h"
              "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
            ];
            Cmd = [ "/bin/python3" ];
            WorkingDir = "/app";
            Volumes = { "/app" = {}; };
          };
        };
      in
      {
        packages = {
          default = streamdb;
          docker = dockerImage;
        };
        
        devShells.default = pkgs.mkShell {
          inputsFrom = [ streamdb ];
          packages = with pkgs; [ cargo rustc rustfmt clippy python311 ];
        };
      }
    );
}
