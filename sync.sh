#!/usr/bin/env bash
# Builds the Redis plugin and installs it into the Tabularis plugins folder.
set -euo pipefail

plugin_src="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

case "$(uname -s)" in
  Linux*)
    PLUGINS_DIR="${XDG_DATA_HOME:-$HOME/.local/share}/tabularis/plugins"
    ;;
  Darwin*)
    PLUGINS_DIR="$HOME/Library/Application Support/com.debba.tabularis/plugins"
    ;;
  MINGW*|MSYS*|CYGWIN*)
    PLUGINS_DIR="${APPDATA}/com.debba.tabularis/plugins"
    ;;
  *)
    echo "Unsupported OS: $(uname -s)" >&2
    exit 1
    ;;
esac

echo "Target plugins directory: $PLUGINS_DIR"
manifest="$plugin_src/manifest.json"
plugin_id=$(grep -o '"id"\s*:\s*"[^"]*"' "$manifest" | head -1 | sed 's/.*: *"\(.*\)"/\1/')
executable=$(grep -o '"executable"\s*:\s*"[^"]*"' "$manifest" | head -1 | sed 's/.*: *"\(.*\)"/\1/')

if [[ -z "$plugin_id" || -z "$executable" ]]; then
  echo "[SKIP] Could not parse manifest.json" >&2
  exit 1
fi

echo "==> Plugin: $plugin_id"
echo "  Building (cargo build --release)..."
cargo build --release --manifest-path "$plugin_src/Cargo.toml"

dest_dir="$PLUGINS_DIR/$plugin_id"
mkdir -p "$dest_dir"
cp "$manifest" "$dest_dir/manifest.json"
echo "  Copied manifest.json"

bin_path="$plugin_src/target/release/$executable"
if [[ -f "$bin_path" ]]; then
  cp "$bin_path" "$dest_dir/$executable"
  chmod +x "$dest_dir/$executable"
  echo "  Copied executable: $executable"
else
  echo "  [WARN] Executable not found: $bin_path" >&2
fi

echo "  Installed to: $dest_dir"
echo "Sync complete. Restart Tabularis to load the plugin."
