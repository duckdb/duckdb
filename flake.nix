{
  description = "Nix flake for duckdb";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = {
    flake-utils,
    nixpkgs,
    ...
  }: let
    systems = ["x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin"];
    outputs = flake-utils.lib.eachSystem systems (system: let
      pkgs = import nixpkgs {
        inherit system;
        overlays = [];
      };
    in {
      # nix fmt
      formatter = pkgs.alejandra;

      # nix develop -c $SHELL
      devShells.default = pkgs.mkShell {
        packages = with pkgs;
          [
            cmake
            libxml2
            ninja
            qemu
            openssl
            zlib
          ]
          ++ (with llvmPackages_16; [
            clang
            clang-unwrapped
            lld
            llvm
          ]);
      };
    });
  in
    outputs;
}
