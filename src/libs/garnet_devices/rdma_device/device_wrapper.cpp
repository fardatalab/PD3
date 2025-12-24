#include "rdma_device.hpp"
#include "null_device.hpp"

#include <string>

#if defined(_WIN32) || defined(_WIN64)  
#define EXPORTED_SYMBOL __declspec(dllexport)  
#else  
#define EXPORTED_SYMBOL __attribute__((visibility("default")))  
#endif 

extern "C" {

  EXPORTED_SYMBOL RdmaDevice* RdmaDevice_Create(const char* file, bool enablePrivileges, bool unbuffered, bool delete_on_close) {
    return new RdmaDevice(file);
  }

  EXPORTED_SYMBOL NullDevice* NullDevice_Create(const char* file, bool enablePrivileges, bool unbuffered, bool delete_on_close) {
    return new NullDevice();
  }

  EXPORTED_SYMBOL void RdmaDevice_Destroy(RdmaDevice* device) {
    delete device;
  }

  EXPORTED_SYMBOL void NullDevice_Destroy(NullDevice* device) {
    delete device;
  }

  EXPORTED_SYMBOL void RdmaDevice_Reset(RdmaDevice* device) {
    device->Reset();
  }

  EXPORTED_SYMBOL void NullDevice_Reset(NullDevice* device) {
    device->Reset();
  }

  EXPORTED_SYMBOL FASTER::core::Status RdmaDevice_ReadAsync(RdmaDevice* device, uint64_t source, void* dest, uint32_t length, FASTER::core::AsyncIOCallback callback, void* context) {
    return device->ReadAsync(source, dest, length, callback, context);
  }

  EXPORTED_SYMBOL FASTER::core::Status NullDevice_ReadAsync(NullDevice* device, uint64_t source, void* dest, uint32_t length, FASTER::core::AsyncIOCallback callback, void* context) {
    return device->ReadAsync(source, dest, length, callback, context);
  }

  EXPORTED_SYMBOL FASTER::core::Status RdmaDevice_WriteAsync(RdmaDevice* device, const void* source, uint64_t dest, uint32_t length, FASTER::core::AsyncIOCallback callback, void* context) {
    return device->WriteAsync(source, dest, length, callback, context);
  }

  EXPORTED_SYMBOL FASTER::core::Status NullDevice_WriteAsync(NullDevice* device, const void* source, uint64_t dest, uint32_t length, FASTER::core::AsyncIOCallback callback, void* context) {
    return device->WriteAsync(source, dest, length, callback, context);
  }

  EXPORTED_SYMBOL uint64_t RdmaDevice_GetFileSize(RdmaDevice* device, uint64_t segment) {
    return device->GetFileSize(segment);
  }

  EXPORTED_SYMBOL uint64_t NullDevice_GetFileSize(NullDevice* device, uint64_t segment) {
    return device->GetFileSize(segment);
  }

  EXPORTED_SYMBOL int RdmaDevice_PollCompletion(RdmaDevice* device, int timeout_secs) {
    return device->ProcessCompletions(timeout_secs);
  }

  EXPORTED_SYMBOL void RdmaDevice_RemoveSegment(RdmaDevice* device, uint64_t segment) {
    device->RemoveSegment(segment);
  }

  EXPORTED_SYMBOL int NullDevice_PollCompletion(NullDevice* device, int timeout_secs) {
    return 0;
  }

  EXPORTED_SYMBOL void NullDevice_RemoveSegment(NullDevice* device, uint64_t segment) {
    return;
  }
}