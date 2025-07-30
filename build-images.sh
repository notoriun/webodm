#!/bin/bash

set -e

DOCKERFILE="Dockerfile.web"
BASE_IMAGE_VERSION="1.0.1"

usage() {
  echo "Usage: $0 -t <image_tag> [-f <dockerfile>] [-b <base_image_version>]"
  echo ""
  echo "  -t <image_tag>     Required. Full image tag (e.g., user/app:latest)"
  echo "  -f <dockerfile>    Optional. Dockerfile to use (default: Dockerfile.web)"
  echo "  -b <base_image_version>    Optional. Base image version to use on build (default: 1.0.1)"
  exit 1
}

while getopts "t:f:b:" opt; do
  case $opt in
    t) TAG="$OPTARG" ;;
    f) DOCKERFILE="$OPTARG" ;;
    b) BASE_IMAGE_VERSION="$OPTARG" ;;
    *) usage ;;
  esac
done

if [ -z "$TAG" ]; then
  echo "❌ Image tag is required."
  usage
fi

TAG_AMD64="${TAG}-amd64"
TAG_ARM64="${TAG}-arm64"

echo "📦 Building images for:"
echo "  🖥️  AMD64 -> $TAG_AMD64"
echo "  🍏 ARM64 -> $TAG_ARM64"
echo ""

echo "🔨 Building amd64..."
podman build --arch amd64 -f "$DOCKERFILE" -t "$TAG_AMD64" --build-arg VER_PGI_INFRA=${BASE_IMAGE_VERSION}-amd64  .
podman rmi notoriun/pgi_infra:${BASE_IMAGE_VERSION}-amd64

echo "🔨 Building arm64..."
podman build --arch arm64 -f "$DOCKERFILE" -t "$TAG_ARM64" --build-arg VER_PGI_INFRA=${BASE_IMAGE_VERSION}-arm64 .
podman rmi notoriun/pgi_infra:${BASE_IMAGE_VERSION}-arm64

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
podman manifest rm "$TAG"

echo "✅ Done. Multiarch image '$TAG' is available on Docker Hub."
