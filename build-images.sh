#!/bin/bash

set -e

DOCKERFILE="Dockerfile.web"

usage() {
  echo "Usage: $0 -t <image_tag> [-f <dockerfile>]"
  echo ""
  echo "  -t <image_tag>     Required. Full image tag (e.g., user/app:latest)"
  echo "  -f <dockerfile>    Optional. Dockerfile to use (default: Dockerfile.web)"
  exit 1
}

while getopts "t:f:" opt; do
  case $opt in
    t) TAG="$OPTARG" ;;
    f) DOCKERFILE="$OPTARG" ;;
    *) usage ;;
  esac
done

if [ -z "$TAG" ]; then
  echo "❌ Image tag is required."
  usage
fi

TAG_AMD64="${TAG%:*}-amd64"
TAG_ARM64="${TAG%:*}-arm64"

echo "📦 Building images for:"
echo "  🖥️  AMD64 -> $TAG_AMD64"
echo "  🍏 ARM64 -> $TAG_ARM64"
echo ""

echo "🔨 Building amd64..."
podman build --arch amd64 -f "$DOCKERFILE" -t "$TAG_AMD64" .

echo "🔨 Building arm64..."
podman build --arch arm64 -f "$DOCKERFILE" -t "$TAG_ARM64" .

echo "🚀 Pushing amd64..."
podman push "$TAG_AMD64"

echo "🚀 Pushing arm64..."
podman push "$TAG_ARM64"

echo "📦 Creating multiarch manifest: $TAG"
podman manifest create "$TAG"
podman manifest add "$TAG" "docker://$TAG_AMD64"
podman manifest add "$TAG" "docker://$TAG_ARM64"

echo "📤 Pushing manifest..."
podman manifest push "$TAG" "docker://$TAG"

echo "✅ Done. Multiarch image '$TAG' is available on Docker Hub."
