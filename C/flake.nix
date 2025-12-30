{
  description = "StreamDB - A lightweight, thread-safe embedded database using reverse trie";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.05";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        version = "2.0.0";
        pname = "streamdb";

        # 1. Core Library (C11)
        streamdb = pkgs.stdenv.mkDerivation {
          inherit pname version;
          src = ./.;
          nativeBuildInputs = [ pkgs.cmake ];
          cmakeFlags = [
            "-DSTREAMDB_BUILD_TESTS=OFF"
            "-DSTREAMDB_BUILD_SHARED=ON"
            "-DSTREAMDB_BUILD_STATIC=ON"
            "-DCMAKE_INSTALL_LIBDIR=lib"
            "-DCMAKE_INSTALL_INCLUDEDIR=include"
          ];
          postInstall = ''
            sed -i "s|libdir=\''${prefix}/|libdir=|g" $out/lib/pkgconfig/streamdb.pc
            sed -i "s|includedir=\''${prefix}/|includedir=|g" $out/lib/pkgconfig/streamdb.pc
          '';
        };

        # 2. Test Suite
        streamdb-tests = pkgs.stdenv.mkDerivation {
          pname = "${pname}-tests";
          inherit version;
          src = ./.;
          nativeBuildInputs = [ pkgs.cmake ];
          cmakeFlags = [
            "-DSTREAMDB_BUILD_TESTS=ON"
            "-DSTREAMDB_BUILD_SHARED=OFF"
            "-DSTREAMDB_BUILD_STATIC=ON"
            "-DSTREAMDB_INSTALL=OFF"
          ];
          installPhase = ''
            mkdir -p $out/bin
            cp test_streamdb $out/bin/
          '';
        };

      in
      {
        packages = rec {
          default = streamdb;
          docker = pkgs.dockerTools.buildLayeredImage {
            name = pname;
            tag = version;
            created = "now"; 

            contents = [ 
              streamdb           
              streamdb-tests     
              pkgs.busybox       
            ];

            config = {
              # FIX: Set working directory to /tmp where writing is allowed
              WorkingDir = "/tmp";
              Cmd = [ "${streamdb-tests}/bin/test_streamdb" ];
              Env = [ 
                "LD_LIBRARY_PATH=/lib" 
                "STREAMDB_VERSION=${version}"
              ];
              # Optional: Expose /tmp as a volume for persistence testing
              Volumes = {
                "/tmp" = {};
              };
            };
          };
        };
      }
    );
}
