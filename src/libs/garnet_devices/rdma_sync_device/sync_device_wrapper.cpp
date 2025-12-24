#include "rdma_sync_device.hpp"

#include <string>

#if defined(_WIN32) || defined(_WIN64)  
#define EXPORTED_SYMBOL __declspec(dllexport)  
#else  
#define EXPORTED_SYMBOL __attribute__((visibility("default")))  
#endif 

extern "C" {

  EXPORTED_SYMBOL RdmaDeviceSync* RdmaDevice_Create(const char* file, bool enablePrivileges, bool unbuffered, bool delete_on_close) {
    return new RdmaDeviceSync(file);
  }

  EXPORTED_SYMBOL void RdmaDevice_Destroy(RdmaDeviceSync* device) {
    delete device;
  }

  EXPORTED_SYMBOL void RdmaDevice_Reset(RdmaDeviceSync* device) {
    device->Reset();
  }

  EXPORTED_SYMBOL FASTER::core::Status RdmaDevice_ReadAsync(RdmaDeviceSync* device, uint64_t source, void* dest, uint32_t length, FASTER::core::AsyncIOCallback callback, void* context) {
    return device->ReadAsync(source, dest, length, callback, context);
  }

  EXPORTED_SYMBOL FASTER::core::Status RdmaDevice_WriteAsync(RdmaDeviceSync* device, const void* source, uint64_t dest, uint32_t length, FASTER::core::AsyncIOCallback callback, void* context) {
    return device->WriteAsync(source, dest, length, callback, context);
  }

  EXPORTED_SYMBOL uint64_t RdmaDevice_GetFileSize(RdmaDeviceSync* device, uint64_t segment) {
    return device->GetFileSize(segment);
  }

  EXPORTED_SYMBOL void RdmaDevice_RemoveSegment(RdmaDeviceSync* device, uint64_t segment) {
    device->RemoveSegment(segment);
  }
}