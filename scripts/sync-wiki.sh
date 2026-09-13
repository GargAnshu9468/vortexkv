#!/usr/bin/env bash
set -e

# ==============================================================================
# 🌌 VortexKV GitHub Wiki Automated Synchronizer
# Usage: ./scripts/sync-wiki.sh
# ==============================================================================

WIKI_DIR="docs/wiki"
TMP_CLONE="/tmp/vortexkv-wiki-sync"
REPO_OWNER="GargAnshu9468"
REPO_NAME="vortexkv"

echo "🔍 Resolving GitHub credentials..."
TOKEN=$(printf "protocol=https\nhost=github.com\n" | git credential fill 2>/dev/null | grep password= | cut -d= -f2-)

if [ -z "$TOKEN" ]; then
    WIKI_URL="https://github.com/${REPO_OWNER}/${REPO_NAME}.wiki.git"
else
    WIKI_URL="https://${TOKEN}@github.com/${REPO_OWNER}/${REPO_NAME}.wiki.git"
fi

rm -rf "$TMP_CLONE"

echo "📡 Checking GitHub Wiki repository..."
if ! git clone "$WIKI_URL" "$TMP_CLONE" >/dev/null 2>&1; then
    echo ""
    echo "⚠️  GitHub has not initialized the wiki git repository yet."
    echo "👉 Step 1: Open https://github.com/${REPO_OWNER}/${REPO_NAME}/wiki in your browser."
    echo "👉 Step 2: Click the green button 'Create the first page' and click 'Save page'."
    echo "👉 Step 3: Run this script again (./scripts/sync-wiki.sh)."
    echo ""
    echo "All 12 documentation pages are ready in docs/wiki/ and will push automatically!"
    exit 0
fi

echo "📦 Copying wiki pages from $WIKI_DIR to wiki repo..."
cp "$WIKI_DIR"/*.md "$TMP_CLONE/"

cd "$TMP_CLONE"
git config user.name "Anshu Garg"
git config user.email "a.kgarg9050@gmail.com"

git add -A
if git diff --cached --quiet; then
    echo "✓ Wiki is already up to date!"
else
    git commit -m "docs(wiki): sync all documentation pages from repository"
    git push origin master
    echo "🎉 Successfully synchronized all wiki pages to https://github.com/${REPO_OWNER}/${REPO_NAME}/wiki"
fi

rm -rf "$TMP_CLONE"
