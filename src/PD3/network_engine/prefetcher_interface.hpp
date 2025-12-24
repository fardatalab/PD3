#pragma once

#include "client_types.hpp"
#include "prefetcher/prefetcher.hpp"

// Global pointer to the prefetcher instance
inline dpf::Prefetcher* PREFETCHER_PTR = nullptr;

// Sets the global prefetcher instance
inline void SetPrefetcher(dpf::Prefetcher* prefetcher) {
  PREFETCHER_PTR = prefetcher;
}

inline dpf::Prefetcher* GetPrefetcher() {
  return PREFETCHER_PTR;
}

// Forwards a client request to the prefetcher, if available
inline bool PassClientRequest(const ClientRequestT& req) {
  return PREFETCHER_PTR && PREFETCHER_PTR->PassClientRequest(req);
}
