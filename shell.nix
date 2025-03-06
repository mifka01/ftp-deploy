{ pkgs ? import <nixpkgs> {} }:
pkgs.mkShell {
  name = "ftp-deploy";
  buildInputs = [
    pkgs.nodejs_22
    pkgs.lftp
  ];
}

