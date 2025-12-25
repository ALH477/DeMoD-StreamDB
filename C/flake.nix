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
        
        # Common build settings
        version = "2.0.0";
        pname = "streamdb";
        
      in
      {
        packages = rec {
          default = streamdb;
          
          streamdb = pkgs.stdenv.mkDerivation {
            inherit pname version;
            src = ./.;

            # No external dependencies - cross-platform UUID is built-in
            nativeBuildInputs = [ pkgs.cmake ];
            
            # Use CMake for building
            cmakeFlags = [
              "-DSTREAMDB_BUILD_TESTS=OFF"
              "-DSTREAMDB_BUILD_SHARED=ON"
              "-DSTREAMDB_BUILD_STATIC=ON"
            ];

            meta = with pkgs.lib; {
              description = "A lightweight, thread-safe embedded database using reverse trie";
              longDescription = ''
                StreamDB is an in-memory key-value store with optional file persistence.
                It uses a reverse trie structure for efficient suffix-based searches.
                Features include thread-safety, binary key support, auto-flush to disk,
                and cross-platform compatibility.
              '';
              homepage = "https://github.com/demod/streamdb";
              license = licenses.lgpl21Plus;
              platforms = platforms.unix ++ platforms.windows;
              maintainers = [ ];
            };
          };

          # Alternative: build with Make instead of CMake
          streamdb-make = pkgs.stdenv.mkDerivation {
            pname = "${pname}-make";
            inherit version;
            src = ./.;

            buildPhase = ''
              runHook preBuild
              
              # Build object files
              $CC -Wall -Wextra -pedantic -std=c11 -O2 -fPIC \
                -Iinclude -c src/streamdb.c -o streamdb.o
              $CC -Wall -Wextra -pedantic -std=c11 -O2 -fPIC \
                -Iinclude -c src/streamdb_wrapper.c -o streamdb_wrapper.o
              
              # Build static library
              ar rcs libstreamdb.a streamdb.o streamdb_wrapper.o
              
              # Build shared library
              $CC -shared -o libstreamdb.so streamdb.o streamdb_wrapper.o -lpthread
              
              runHook postBuild
            '';

            installPhase = ''
              runHook preInstall
              
              mkdir -p $out/{lib,include}
              cp libstreamdb.a libstreamdb.so $out/lib/
              cp include/streamdb.h include/streamdb_wrapper.h $out/include/
              
              # Create pkg-config file
              mkdir -p $out/lib/pkgconfig
              cat > $out/lib/pkgconfig/streamdb.pc << EOF
              prefix=$out
              exec_prefix=\''${prefix}
              libdir=\''${prefix}/lib
              includedir=\''${prefix}/include

              Name: StreamDB
              Description: A lightweight, thread-safe embedded database using reverse trie
              Version: ${version}
              Libs: -L\''${libdir} -lstreamdb -lpthread
              Cflags: -I\''${includedir}
              EOF
              
              runHook postInstall
            '';

            meta = with pkgs.lib; {
              description = "A lightweight, thread-safe embedded database using reverse trie";
              license = licenses.lgpl21Plus;
              platforms = platforms.unix;
            };
          };

          # Tests package
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

            meta.description = "StreamDB test suite";
          };
        };

        # Development shell
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            gcc
            cmake
            gnumake
            gdb
            valgrind
          ];

          shellHook = ''
            echo "StreamDB ${version} development environment"
            echo ""
            echo "Build commands:"
            echo "  make          - Build static and shared libraries"
            echo "  make test     - Build and run tests"
            echo "  make debug    - Build with sanitizers and run tests"
            echo ""
            echo "Or use CMake:"
            echo "  mkdir build && cd build"
            echo "  cmake .."
            echo "  cmake --build ."
            echo "  ctest"
          '';
        };

        # Expose check for CI
        checks.default = self.packages.${system}.streamdb-tests;
      }
    );
}
