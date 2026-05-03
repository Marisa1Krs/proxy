# Proxy Project

## 项目简介

基于 C 语言开发的代理项目。

## 目录结构

```
Proxy/
├── include/       # 头文件目录
├── src/           # 源代码目录
├── config/        # 配置文件目录
├── log/           # 日志文件目录（已 gitignore）
├── build/         # 构建输出目录（已 gitignore）
├── CMakeLists.txt # CMake 构建配置
└── README.md      # 项目说明
```

## 构建方式

```bash
mkdir -p build && cd build
cmake ..
make
```

## 配置说明

配置文件位于 `config/` 目录下。
