#include "redy_device_single.hpp"

#include <string>

#if defined(_WIN32) || defined(_WIN64)  
#define EXPORTED_SYMBOL __declspec(dllexport)  
#else  
#define EXPORTED_SYMBOL __attribute__((visibility("default")))  
#endif 

extern "C" {

  EXPORTED_SYMBOL RedyDeviceSingle* RedyDeviceSingle_Create(const char* file, bool enablePrivileges, bool unbuffered, bool delete_on_close) {
    return new RedyDeviceSingle(file);
  }

  EXPORTED_SYMBOL void RedyDeviceSingle_Destroy(RedyDeviceSingle* device) {
    delete device;
  }

  EXPORTED_SYMBOL void RedyDeviceSingle_Reset(RedyDeviceSingle* device) {
    device->Reset();
  }

  EXPORTED_SYMBOL FASTER::core::Status RedyDeviceSingle_ReadAsync(RedyDeviceSingle* device, uint64_t source, void* dest, uint32_t length, FASTER::core::AsyncIOCallback callback, void* context) {
    return device->ReadAsync(source, dest, length, callback, context);
  }

  EXPORTED_SYMBOL FASTER::core::Status RedyDeviceSingle_WriteAsync(RedyDeviceSingle* device, const void* source, uint64_t dest, uint32_t length, FASTER::core::AsyncIOCallback callback, void* context) {
    return device->WriteAsync(source, dest, length, callback, context);
  }

//   EXPORTED_SYMBOL bool RdmaDevice_TryComplete(RdmaDeviceSync* device) {
//     return device->TryComplete();
//   }

  EXPORTED_SYMBOL uint64_t RedyDeviceSingle_GetFileSize(RedyDeviceSingle* device, uint64_t segment) {
    return device->GetFileSize(segment);
  }

//   EXPORTED_SYMBOL int RdmaDevice_PollCompletion(RdmaDeviceSync* device, int timeout_secs) {
//     return device->ProcessCompletions(timeout_secs);
//   }

  EXPORTED_SYMBOL void RedyDeviceSingle_RemoveSegment(RedyDeviceSingle* device, uint64_t segment) {
    device->RemoveSegment(segment);
  }
}