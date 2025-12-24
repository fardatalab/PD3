#include <rigtorp/SPSCQueue.h>

// A single-producer single-consumer lock-free queue for passing messages from
// one thread to another.
template<typename T>
using MessageQueue = rigtorp::SPSCQueue<T>;
