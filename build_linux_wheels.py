#!/usr/bin/env python3
"""
Build Linux wheels for drainage package using cibuildwheel
"""

import subprocess
import sys
import os

def run_command(cmd, description):
    print(f"🔄 {description}...")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"❌ Error: {result.stderr}")
        return False
    print(f"✅ {description} completed")
    return True

def main():
    print("🚀 Building Linux wheels for drainage...")
    
    # Install cibuildwheel
    if not run_command("pip install cibuildwheel", "Installing cibuildwheel"):
        return False
    
    # Set environment variables for Linux builds
    os.environ["CIBW_BUILD"] = "cp39-* cp310-* cp311-* cp312-*"
    os.environ["CIBW_PLATFORM"] = "linux"
    os.environ["CIBW_BEFORE_BUILD"] = "pip install maturin"
    
    # Build wheels
    if not run_command("cibuildwheel --platform linux", "Building Linux wheels"):
        return False
    
    print("🎉 Linux wheels built successfully!")
    print("📦 Wheels are in the wheelhouse/ directory")
    
    # List the built wheels
    if os.path.exists("wheelhouse"):
        print("\n📋 Built wheels:")
        for file in os.listdir("wheelhouse"):
            if file.endswith(".whl"):
                print(f"  - {file}")

if __name__ == "__main__":
    main()
