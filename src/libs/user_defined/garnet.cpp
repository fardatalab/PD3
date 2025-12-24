// A sample user_defined.hpp for the Garnet app

#include "garnet.hpp"

#include <cstdint>


// Helpers for Garnet's ParseRemoteWrite

// Reference:
// https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs
struct __attribute__((packed)) RecordInfo
{
  static constexpr inline uint64_t FILLER_BIT_OFFSET = 60;

  bool is_invalid() const
  {
    // Tsavorite maintains an invariant that unused space is zeroed.
    // Reference:
    // https://microsoft.github.io/garnet/docs/dev/tsavorite/reviv#ensuring-log-integrity
    //
    // So if we encounter a zero-ed RecordInfo, it means that there's no record
    // here.
    return val == 0;
  }

  bool has_filler() const { return val & (uint64_t{ 1 } << FILLER_BIT_OFFSET); }

  uint64_t val;
};

// Reference:
// https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/VarLen/SpanByte.cs
struct __attribute__((packed)) SpanByte
{
  size_t total_size() const { return sizeof(length) + length; }

  uint32_t length; // assume no header
  char payload[];
};

// Reference:
// https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Constants.cs
inline constexpr size_t RECORD_ALIGNMENT = 8;

static inline size_t
round_up(size_t val, size_t alignment)
{
  // alignment must be a power of 2
  return (val + (alignment - 1)) & ~(alignment - 1);
}

// Reference:
// https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Allocator/SpanByteAllocatorImpl.cs
// Calculations of offsets in record:
static inline const RecordInfo*
get_record_info(const char* record_ptr)
{
  return reinterpret_cast<const RecordInfo*>(record_ptr);
}

static inline const char*
get_key_ptr(const char* record_ptr)
{
  return record_ptr + sizeof(RecordInfo);
}

static inline const SpanByte*
get_key(const char* record_ptr)
{
  return reinterpret_cast<const SpanByte*>(get_key_ptr(record_ptr));
}

static inline size_t
aligned_key_size(const char* record_ptr)
{
  return round_up(get_key(record_ptr)->total_size(), RECORD_ALIGNMENT);
}

static inline const char*
get_value_ptr(const char* record_ptr)
{
  return get_key_ptr(record_ptr) + aligned_key_size(record_ptr);
}

static inline const SpanByte*
get_value(const char* record_ptr)
{
  return reinterpret_cast<const SpanByte*>(get_value_ptr(record_ptr));
}

static inline size_t
get_record_size(const char* record_ptr)
{
  size_t value_len = get_value(record_ptr)->total_size();
  if (get_record_info(record_ptr)->has_filler()) {
    const char* extra_value_len_ptr =
      get_value_ptr(record_ptr) + round_up(value_len, sizeof(uint32_t));
    size_t extra_value_len =
      *reinterpret_cast<const uint32_t*>(extra_value_len_ptr);

    value_len += extra_value_len;
  }

  return round_up(sizeof(RecordInfo) + aligned_key_size(record_ptr) + value_len,
                  RECORD_ALIGNMENT);
}

int ParseRemoteWrite(const char* data, size_t len, uint64_t* keys)
{
  // local_addr points to the start of the page Garnet is flushing, and len
  // is the page size.
  // This flushing is initiated by AllocatorBase::OnPagesMarkedReadOnly() in
  // Garnet.
  size_t keys_idx = 0;

  size_t offset = 0;
  while (((len - offset) >= sizeof(RecordInfo)) &&
         get_record_info(data + offset)->is_invalid()) {
    // Handles the case where the start of the page is zero-ed.
    // This happens because Garnet starts from a non-zero offset
    // (Constants.kFirstValidAddress in Garnet) for its first page.
    offset += sizeof(RecordInfo);
  }

  while ((len - offset) >= sizeof(RecordInfo)) {
    const char* record_ptr = data + offset;
    auto* record_info = get_record_info(record_ptr);

    if (record_info->is_invalid()) {
      // Handles the case where the end of the page is zero-ed.
      // This happens when a record can't fit into the end of the page,
      // and Garnet moves onto the next page instead.
      break;
    }

    // assumes record is correctly laid out
    auto* key = get_key(record_ptr);
    size_t record_size = get_record_size(record_ptr);

    keys[keys_idx++] = *reinterpret_cast<const uint64_t*>(key->payload);

    offset += record_size;
  }

  return keys_idx;
}
