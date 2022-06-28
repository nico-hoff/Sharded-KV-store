#include <netdb.h>

// NOLINTNEXTLINE(cert-err58-cpp, concurrency-mt-unsafe,
// cppcoreguidelines-avoid-non-const-global-variables)
hostent *hostip = gethostbyname("localhost");
