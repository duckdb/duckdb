#!/usr/bin/env bash

set -euo pipefail

existing_s5cmd_path="$(which s5cmd 2>/dev/null || true)"
if [[ -n "${existing_s5cmd_path}" ]]; then
  set -x
  s5cmd version
  exit 0
fi

S5CMD_VERSION="${S5CMD_VERSION:-2.3.0}"
S5CMD_TAG="v${S5CMD_VERSION}"
S5CMD_BASE_URL="https://github.com/peak/s5cmd/releases/download/${S5CMD_TAG}"

if [ -n "${CI:-}" ]; then
  INSTALL_DIR="${S5CMD_INSTALL_DIR:-/usr/bin}"
else
  INSTALL_DIR="${S5CMD_INSTALL_DIR:-$HOME/.local/bin}"
fi

uname_s="$(uname -s)"
uname_m="$(uname -m)"

case "${uname_s}" in
  Linux)
    case "${uname_m}" in
      x86_64|amd64) asset="s5cmd_${S5CMD_VERSION}_Linux-64bit.tar.gz" ;;
      aarch64|arm64) asset="s5cmd_${S5CMD_VERSION}_Linux-arm64.tar.gz" ;;
      *)
        echo "Unsupported Linux architecture for s5cmd: ${uname_m}" >&2
        exit 1
        ;;
    esac
    ;;
  Darwin)
    case "${uname_m}" in
      x86_64|amd64) asset="s5cmd_${S5CMD_VERSION}_macOS-64bit.tar.gz" ;;
      arm64) asset="s5cmd_${S5CMD_VERSION}_macOS-arm64.tar.gz" ;;
      *)
        echo "Unsupported macOS architecture for s5cmd: ${uname_m}" >&2
        exit 1
        ;;
    esac
    ;;
  MINGW*|MSYS*|CYGWIN*)
    case "${uname_m}" in
      x86_64|amd64) asset="s5cmd_${S5CMD_VERSION}_Windows-64bit.zip" ;;
      arm64|aarch64) asset="s5cmd_${S5CMD_VERSION}_Windows-arm64.zip" ;;
      *)
        echo "Unsupported Windows architecture for s5cmd: ${uname_m}" >&2
        exit 1
        ;;
    esac
    ;;
  *)
    echo "Unsupported OS for s5cmd install: ${uname_s}" >&2
    exit 1
    ;;
esac

workdir="$(mktemp -d "${TMPDIR:-/tmp}/s5cmd-install.XXXXXX")"
cleanup() {
  rm -rf "${workdir}"
}
trap cleanup EXIT

archive_path="${workdir}/${asset}"
checksums_path="${workdir}/s5cmd_checksums.txt"

if command -v curl >/dev/null 2>&1; then
  curl -fsSL "${S5CMD_BASE_URL}/${asset}" -o "${archive_path}"
  curl -fsSL "${S5CMD_BASE_URL}/s5cmd_checksums.txt" -o "${checksums_path}"
elif command -v wget >/dev/null 2>&1; then
  wget -q "${S5CMD_BASE_URL}/${asset}" -O "${archive_path}"
  wget -q "${S5CMD_BASE_URL}/s5cmd_checksums.txt" -O "${checksums_path}"
else
  echo "Neither curl nor wget found for downloading s5cmd" >&2
  exit 1
fi

expected_line="$(grep " ${asset}$" "${checksums_path}" || true)"
if [[ -z "${expected_line}" ]]; then
  echo "Failed to find checksum entry for ${asset}" >&2
  exit 1
fi
expected_sha256="$(printf '%s' "${expected_line}" | awk '{print $1}')"

if command -v shasum >/dev/null 2>&1; then
  actual_sha256="$(shasum -a 256 "${archive_path}" | awk '{print $1}')"
elif command -v sha256sum >/dev/null 2>&1; then
  actual_sha256="$(sha256sum "${archive_path}" | awk '{print $1}')"
else
  actual_sha256="$(openssl dgst -sha256 "${archive_path}" | awk '{print $NF}')"
fi

if [[ "${actual_sha256}" != "${expected_sha256}" ]]; then
  echo "Checksum mismatch for ${asset}" >&2
  echo "expected: ${expected_sha256}" >&2
  echo "actual:   ${actual_sha256}" >&2
  exit 1
fi

mkdir -p "${workdir}/extract"
if [[ "${archive_path}" == *.tar.gz ]]; then
  tar -xzf "${archive_path}" -C "${workdir}/extract"
else
  if command -v unzip >/dev/null 2>&1; then
    unzip -q "${archive_path}" -d "${workdir}/extract"
  elif [[ -x "/c/Program Files/7-Zip/7z.exe" ]]; then
    "/c/Program Files/7-Zip/7z.exe" x -y "${archive_path}" "-o${workdir}/extract" >/dev/null
  elif command -v powershell >/dev/null 2>&1; then
    powershell -NoProfile -Command "Expand-Archive -Path '${archive_path}' -DestinationPath '${workdir}/extract' -Force" >/dev/null
  else
    echo "No extractor found for zip archive: ${archive_path}" >&2
    exit 1
  fi
fi

chmod +x "${workdir}/extract/s5cmd"

mkdir -p "${INSTALL_DIR}"
if [[ "${uname_s}" == "Linux" && -n "${CI:-}" ]] && command -v sudo >/dev/null 2>&1; then
  sudo cp "${workdir}/extract/s5cmd" "${INSTALL_DIR}/s5cmd"
else
  cp "${workdir}/extract/s5cmd" "${INSTALL_DIR}/s5cmd"
fi

if [[ ":$PATH:" != *":${INSTALL_DIR}:"* ]]; then
  export PATH="${INSTALL_DIR}:$PATH"
fi

if [[ -n "${GITHUB_PATH:-}" ]]; then
  echo "${INSTALL_DIR}" >> "${GITHUB_PATH}"
fi

set -x
"${INSTALL_DIR}/s5cmd" version
