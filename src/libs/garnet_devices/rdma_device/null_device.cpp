#include "null_device.hpp"

#include <iostream>

NullDevice::NullDevice() {}

NullDevice::~NullDevice() {}


FASTER::core::Status NullDevice::ReadAsync(uint64_t source, 
                                          void* dest, 
                                          uint32_t length, 
                                          FASTER::core::AsyncIOCallback callback, 
                                          void* context) 
{
  std::cout << "NullDevice::ReadAsync: source: " << source << ", dest: " << dest << ", length: " << length << std::endl;
  callback((FASTER::core::IAsyncContext*)context, FASTER::core::Status::Ok, length);
  return FASTER::core::Status::Ok;
}

FASTER::core::Status NullDevice::WriteAsync(const void* source, uint64_t dest, 
                                            uint32_t length, 
                                            FASTER::core::AsyncIOCallback callback, 
                                            void* context) 
{
  std::cout << "NullDevice::WriteAsync: source: " << source << ", dest: " << dest << ", length: " << length << std::endl;
  callback((FASTER::core::IAsyncContext*)context, FASTER::core::Status::Ok, length);
  return FASTER::core::Status::Ok;
}