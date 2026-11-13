# gRPC

This repository provides a base for creating project-specific Python gRPC modules.
It is intended to keep common gRPC project structure, packaging, and generated-code workflows consistent across projects.

## Building

Build systems are generated using CMake. Configuration presets are provided to standardize the CMake configuration 
process for the target `(architecture, platform, build type)` triplet.

```sh
cmake -S . --preset x64-windows-debug
```

```sh
cmake -S . --preset arm64-osx-debug
```

## 📄 License and Legal Notices

© 2026 CCP Games

This software is provided by CCP Games and does not include or distribute any third-party libraries or frameworks.

This software provides a base for creating project specific Python gRPC modules.

Trademark Notice: CCP Games is a trademark of CCP ehf.

This project is licensed under the [MIT License](LICENSE.md). Nothing in the [MIT License](LICENSE.md) grants any rights to CCP Games' trademarks or game content.
