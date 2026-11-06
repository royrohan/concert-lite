#!/usr/bin/env bash
# Start the Docker daemon if it isn't running, wait for it, then run scripts/up.sh with the same args:
#   scripts/up-with-docker.sh spanner --analytics --trading
#   scripts/up-with-docker.sh down
# Supports Docker Desktop (macOS: `open -a Docker`; Linux: systemctl), OrbStack and Colima.
# DOCKER_WAIT_SECS (default 120) bounds the wait for the daemon.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
wait_secs=${DOCKER_WAIT_SECS:-120}

if ! command -v docker >/dev/null 2>&1; then
  echo "docker CLI not found on PATH" >&2
  exit 1
fi

if docker info >/dev/null 2>&1; then
  echo "Docker daemon already running"
else
  echo "Starting Docker..."
  case "$(uname -s)" in
    Darwin)
      if command -v orb >/dev/null 2>&1; then
        orb start
      elif command -v colima >/dev/null 2>&1; then
        colima start
      elif [ -d /Applications/Docker.app ] || [ -d "$HOME/Applications/Docker.app" ]; then
        open -a Docker
      else
        echo "No Docker runtime found (Docker Desktop, OrbStack or Colima)" >&2
        exit 1
      fi
      ;;
    Linux)
      if command -v systemctl >/dev/null 2>&1; then
        systemctl --user start docker-desktop 2>/dev/null || sudo systemctl start docker
      else
        sudo service docker start
      fi
      ;;
    *)
      echo "Unsupported OS $(uname -s); start Docker manually" >&2
      exit 1
      ;;
  esac

  printf "Waiting for Docker daemon"
  for ((i = 0; i < wait_secs; i += 2)); do
    if docker info >/dev/null 2>&1; then
      echo " ready"
      break
    fi
    printf "."
    sleep 2
  done
  if ! docker info >/dev/null 2>&1; then
    echo
    echo "Docker daemon did not become ready within ${wait_secs}s" >&2
    exit 1
  fi
fi

exec "$here/up.sh" "$@"
