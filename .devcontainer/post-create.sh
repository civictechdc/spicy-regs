#!/usr/bin/env bash
# Runs once when the devcontainer / Codespace is built.
set -euo pipefail

echo "==> Installing uv"
curl -LsSf https://astral.sh/uv/install.sh | sh

# Make uv available in this shell and future ones.
export PATH="$HOME/.local/bin:$PATH"
if ! grep -q 'HOME/.local/bin' "$HOME/.bashrc" 2>/dev/null; then
    echo 'export PATH="$HOME/.local/bin:$PATH"' >> "$HOME/.bashrc"
fi

echo "==> Installing project dependencies"
uv sync

echo "==> Setup complete. Try: uv run pytest"
