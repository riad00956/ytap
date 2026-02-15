#!/usr/bin/env bash
# Exit on error
set -o errexit

# Install dependencies
pip install -r requirements.txt

# Create a directory for binaries
mkdir -p bin

# Download and install FFmpeg (Static Build)
echo "Downloading FFmpeg..."
curl -L https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-linux64-gpl.tar.xz -o ffmpeg.tar.xz
tar -xf ffmpeg.tar.xz
mv ffmpeg-master-latest-linux64-gpl/bin/ffmpeg bin/ffmpeg
mv ffmpeg-master-latest-linux64-gpl/bin/ffprobe bin/ffprobe

# Cleanup
rm -rf ffmpeg-master-latest-linux64-gpl ffmpeg.tar.xz

# Make executables
chmod +x bin/ffmpeg
chmod +x bin/ffprobe

# Add bin to PATH
export PATH=$PWD/bin:$PATH

echo "Build and FFmpeg installation complete!"
