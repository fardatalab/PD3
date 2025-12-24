#include "redy_device.hpp"

#include <string>

#if defined(_WIN32) || defined(_WIN64)
#define EXPORTED_SYMBOL __declspec(dllexport)
#else
#define EXPORTED_SYMBOL __attribute__((visibility("default")))
#endif

extern "C" {

  EXPORTED_SYMBOL RedyDevice* RedyDevice_Create(const char* file, bool enablePrivileges, bool unbuffered, bool delete_on_close) {
    return new RedyDevice(file);
  }

  EXPORTED_SYMBOL void RedyDevice_Destroy(RedyDevice* device) {
    delete device;
  }

  EXPORTED_SYMBOL void RedyDevice_Reset(RedyDevice* device) {
    device->Reset();
  }

  EXPORTED_SYMBOL FASTER::core::Status RedyDevice_ReadAsync(RedyDevice* device, uint64_t source, void* dest, uint32_t length, FASTER::core::AsyncIOCallback callback, void* context) {
    return device->ReadAsync(source, dest, length, callback, context);
  }

  EXPORTED_SYMBOL FASTER::core::Status RedyDevice_WriteAsync(RedyDevice* device, const void* source, uint64_t dest, uint32_t length, FASTER::core::AsyncIOCallback callback, void* context) {
    return device->WriteAsync(source, dest, length, callback, context);
  }

  EXPORTED_SYMBOL uint64_t RedyDevice_GetFileSize(RedyDevice* device, uint64_t segment) {
    return device->GetFileSize(segment);
  }

  EXPORTED_SYMBOL void RedyDevice_RemoveSegment(RedyDevice* device, uint64_t segment) {
    device->RemoveSegment(segment);
  }
}
