#!/bin/bash
# Build PyJWT[cryptography] Lambda layer for Analytics_Dashboard_API
# Run this locally, then attach the layer ARN to the Lambda function.
#
# Usage:
#   cd Lambdas/lambda/Analytics_Dashboard_API
#   bash build-layer.sh
#   aws lambda publish-layer-version \
#     --layer-name pyjwt-cryptography \
#     --zip-file fileb://pyjwt-layer.zip \
#     --compatible-runtimes python3.13 \
#     --profile chris-admin
#   # Then attach the layer ARN to the Analytics_Dashboard_API Lambda

set -euo pipefail

LAYER_DIR=$(mktemp -d)
pip install -r requirements-layer.txt -t "$LAYER_DIR/python" --platform manylinux2014_x86_64 --only-binary=:all:
cd "$LAYER_DIR"
zip -r9 "$OLDPWD/pyjwt-layer.zip" python
cd "$OLDPWD"
rm -rf "$LAYER_DIR"
echo "✅ Layer built: pyjwt-layer.zip"
