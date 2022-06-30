rm -rf build

make -C rocksdb static_lib -j$(nproc)


cmake -S . -B build/dev -D CMAKE_BUILD_TYPE=Release
cmake --build build/dev -j$(nproc)
