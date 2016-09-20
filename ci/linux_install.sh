#!/bin/sh

sudo apt-get update -qq
sudo apt-get install -qq g++-4.8
sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test

if [[ "$TRAVIS_PYTHON_VERSION" == "2.7" ]]; then
    wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh -O miniconda.sh;
else
    wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh;

echo "g++-4.8" > .cxx.choice

