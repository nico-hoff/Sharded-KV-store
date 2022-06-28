if(PROJECT_IS_TOP_LEVEL)
  set(CMAKE_INSTALL_INCLUDEDIR include/clt-svr-model CACHE PATH "")
endif()

include(CMakePackageConfigHelpers)
include(GNUInstallDirs)

# find_package(<package>) call for consumers to find this project
set(package clt-svr-model)

install(
    TARGETS clt svr
    EXPORT clt-svr-modelTargets
    RUNTIME COMPONENT clt-svr-model_Runtime
)

write_basic_package_version_file(
    "${package}ConfigVersion.cmake"
    COMPATIBILITY SameMajorVersion
)

# Allow package maintainers to freely override the path for the configs
set(
    clt-svr-model_INSTALL_CMAKEDIR "${CMAKE_INSTALL_DATADIR}/${package}"
    CACHE PATH "CMake package config location relative to the install prefix"
)
mark_as_advanced(clt-svr-model_INSTALL_CMAKEDIR)

install(
    FILES cmake/install-config.cmake
    DESTINATION "${clt-svr-model_INSTALL_CMAKEDIR}"
    RENAME "${package}Config.cmake"
    COMPONENT clt-svr-model_Development
)

install(
    FILES "${PROJECT_BINARY_DIR}/${package}ConfigVersion.cmake"
    DESTINATION "${clt-svr-model_INSTALL_CMAKEDIR}"
    COMPONENT clt-svr-model_Development
)

install(
    EXPORT clt-svr-modelTargets
    NAMESPACE clt-svr-model::
    DESTINATION "${clt-svr-model_INSTALL_CMAKEDIR}"
    COMPONENT clt-svr-model_Development
)

if(PROJECT_IS_TOP_LEVEL)
  include(CPack)
endif()
