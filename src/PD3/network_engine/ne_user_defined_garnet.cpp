#include <assert.h>
#include <limits.h>
#include <setjmp.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ne_user_defined.hpp"
#include "prefetcher_interface.hpp"

// Note: These functions/structures deeply mirror the way that garnet parses the
// messages/packets. It is available here:
// https://github.com/microsoft/garnet/tree/b7071dc22efe7206414376e45955629a88444adf

#define MIN_PARAMS 5  // Minimum parameters to allocate initially

namespace {

/*
 *  Represents contiguous region of arbitrary _pinned_ memory.
 */
typedef struct {
  int length;  // length of ArgSlice
  char* ptr;   // Pointer to parsed argument
} ArgSlice;

/*
 *  Wrapper to hold parse state for a RESP session.
 */
typedef struct {
  int count;         // Count of accessible arguments for the command
  ArgSlice* buffer;  // Pointer to the allocated buffer
  int lenBuffer;
} SessionParseState;

/*
 * A Resp session.
 */
typedef struct {
  char* recvBufferPtr;  // input buffer
  int bytesRead;        // total size of incoming data
  int readHead;     // number of bytes successfully read from the input buffer
  int endReadHead;  // End of the current command, after successful parsing.
  int opCount;      // number of commands processed
  jmp_buf exceptionBuffer;
  SessionParseState parseState;
} RespServerSession;

/*
 * Initialise all elements of the SessionParseState struct.
 */
void InitializeParseState(SessionParseState* state) {
  state->count = 0;

  state->buffer = (ArgSlice*)calloc(MIN_PARAMS, sizeof(ArgSlice));
  if (!state->buffer) {
    perror("Failed to allocate memory");
    exit(EXIT_FAILURE);
  }
  state->lenBuffer = MIN_PARAMS;
}

/*
 * Initialise all elements of the RespServerSession struct.
 */
void InitializeRespServerSession(RespServerSession* session) {
  session->recvBufferPtr = NULL;
  session->bytesRead = 0;
  session->readHead = 0;
  session->endReadHead = 0;
  session->opCount = 0;

  // Initialize parse state
  InitializeParseState(&session->parseState);
}

/*
 * Free the SessionParseState struct.
 */
void FreeParseState(SessionParseState* state) {
  free(state->buffer);
  state->buffer = NULL;
}

/*
 * Free the RespServerSession passed in.
 */
void FreeRespServerSession(RespServerSession* session) {
  FreeParseState(&session->parseState);
}

/*
 * Exception handling.
 */
void HandleRespParsingException(RespServerSession* session,
                                const char* message) {
  fprintf(stderr, "RESP Parsing Exception: %s\n", message);
}

/*
 * Error handling for integer overflow.
 */
void ThrowIntegerOverflow(const char* position, size_t bytes_read) {
  printf("Integer overflow at position %s, bytes read: %zu\n", position,
         bytes_read);
  exit(1);
}

/*
 * Error handling for unexpected token.
 */
void ThrowUnexpectedToken(char token) {
  printf("Unexpected token: %c\n", token);
  exit(1);
}

/*
 * Fast-parses for command type for one message, starting at the current read
 * head in the receive buffer and advances the read head to the position after
 * the parsed command. Returns the RespCommand that was parsed or
 * RESP_COMMAND_NONE, if no command was matched in this pass. session: Current
 * RespServerSession being parsed. count: Set to the number of arguments stored
 * with the command. Set to -1 if no command found.
 */
RespCommand FastParseCommand(RespServerSession* session, int* count) {
  uint8_t* ptr = (uint8_t*)session->recvBufferPtr + session->readHead;
  int remainingBytes = session->bytesRead - session->readHead;

  uint64_t mask = 0xFFFF00FFFFFF00FF;

  // Check for the initial pattern "*_\r\n$_\r\n"
  if (remainingBytes >= 8 &&
      (*(uint64_t*)ptr & mask) == *((uint64_t*)"*\0\r\n$\0\r\n")) {
    // Extract total element count from the array header
    *count =
        ptr[1] -
        '1';  // NOTE: Subtracting one to account for first token being parsed.
    assert(*count >= 0 && *count < 9);

    // Extract length of the first string header
    uint8_t length = ptr[5] - '0';
    assert(length > 0 && length <= 9);

    int oldReadHead = session->readHead;

    // Ensure the complete command string is contained in the packet
    if (remainingBytes >= length + 10) {
      session->readHead +=
          length +
          10;  // Optimistically advance readhead to the end of the command name
      uint64_t lastWord = *(uint64_t*)(ptr + length + 2);

      switch ((*count << 4) | length) {
        // Commands without arguments
        case 4:
          if (memcmp(&lastWord, "\r\nPING\r\n", 8) == 0) {
            // fprintf(stderr, "PING\n");
            return PING;
          }
          if (memcmp(&lastWord, "\r\nEXEC\r\n", 8) == 0) {
            // fprintf(stderr, "EXEC\n");
            return EXEC;
          }
          break;
        case 5:
          if (memcmp(&lastWord, "\nMULTI\r\n", 8) == 0) {
            // fprintf(stderr, "MULTI\n");
            return MULTI;
          }
          break;
        case 6:
          if (memcmp(&lastWord, "ASKING\r\n", 8) == 0) {
            // fprintf(stderr, "ASKING\n");
            return ASKING;
          }
          break;
        case 7:
          if (memcmp(&lastWord, "ISCARD\r\n", 8) == 0 && ptr[8] == 'D') {
            // fprintf(stderr, "DISCARD\n");
            return DISCARD;
          }
          if (memcmp(&lastWord, "NWATCH\r\n", 8) == 0 && ptr[8] == 'U') {
            // fprintf(stderr, "UNWATCH\n");
            return UNWATCH;
          }
          break;
        case 8:
          if (memcmp(&lastWord, "ADONLY\r\n", 8) == 0 &&
              memcmp(ptr + 8, "RE", 2) == 0) {
            // fprintf(stderr, "READONLY\n");
            return READONLY;
          }
          break;
        case 9:
          if (memcmp(&lastWord, "DWRITE\r\n", 8) == 0 &&
              memcmp(ptr + 8, "READ", 4) == 0) {
            // fprintf(stderr, "READWRITE\n");
            return READWRITE;
          }
          break;

        // Commands with fixed number of arguments
        case (1 << 4) | 3:
          if (memcmp(&lastWord, "3\r\nGET\r\n", 8) == 0) {
            // fprintf(stderr, "GET\n");
            return GET;
          }
          if (memcmp(&lastWord, "3\r\nDEL\r\n", 8) == 0) {
            // fprintf(stderr, "DEL\n");
            return DEL;
          }
          if (memcmp(&lastWord, "3\r\nTTL\r\n", 8) == 0) {
            // fprintf(stderr, "TTL\n");
            return TTL;
          }
          break;

        case (1 << 4) | 4:
          if (memcmp(&lastWord, "\r\nDUMP\r\n", 8) == 0) {
            // fprintf(stderr, "DUMP\n");
            return DUMP;
          }
          if (memcmp(&lastWord, "\r\nINCR\r\n", 8) == 0) {
            // fprintf(stderr, "INCR\n");
            return INCR;
          }
          if (memcmp(&lastWord, "\r\nPTTL\r\n", 8) == 0) {
            // fprintf(stderr, "PTTL\n");
            return PTTL;
          }
          if (memcmp(&lastWord, "\r\nDECR\r\n", 8) == 0) {
            // fprintf(stderr, "DECR\n");
            return DECR;
          }
          break;

        case (1 << 4) | 6:
          if (memcmp(&lastWord, "EXISTS\r\n", 8) == 0) {
            // fprintf(stderr, "EXISTS\n");
            return EXISTS;
          }
          if (memcmp(&lastWord, "GETDEL\r\n", 8) == 0) {
            // fprintf(stderr, "GELDEL\n");
            return GETDEL;
          }
          break;

        case (1 << 4) | 7:
          if (memcmp(&lastWord, "ERSIST\r\n", 8) == 0 && ptr[8] == 'P') {
            // fprintf(stderr, "PERSIST\n");
            return PERSIST;
          }
          if (memcmp(&lastWord, "FCOUNT\r\n", 8) == 0 && ptr[8] == 'P') {
            // fprintf(stderr, "PFCOUNT\n");
            return PFCOUNT;
          }
          break;

        case (2 << 4) | 3:
          if (memcmp(&lastWord, "3\r\nSET\r\n", 8) == 0) {
            // fprintf(stderr, "SET\n");
            return SET;
          }
          break;

          // Add more cases here - omitted for now
      }
    }

    // If no command matched, revert the read head
    session->readHead = oldReadHead;
  }

  // Could not find a matching command, try to handle inline commands - skipping
  // this for now return FastParseInlineCommand(session, &count);

  // no matching command found
  *count = -1;
  return RESP_COMMAND_NONE;
}

/*
 * Initialize the parse state with a given count of arguments
 * count: Size of argument array to allocate.
 */
void InitializeParseStateCount(SessionParseState* state, int count) {
  state->count = count;

  // Check if the buffer is already allocated and if the size is sufficient
  if (state->buffer != NULL &&
      (count <= MIN_PARAMS || count <= state->lenBuffer)) {
    return;  // No need to reallocate
  }

  int sizeToAllocate = (count <= MIN_PARAMS) ? MIN_PARAMS : count;

  // Allocate memory for the buffer
  state->buffer = (ArgSlice*)malloc(sizeToAllocate * sizeof(ArgSlice));

  if (!state->buffer) {
    perror("Failed to allocate memory");
    exit(EXIT_FAILURE);
  }

  state->lenBuffer = sizeToAllocate;
}

/*
 * Read signed 64 bit integer.
 */
bool TryReadUint64(char** ptr, char* end, uint64_t* value, size_t* bytes_read) {
  *bytes_read = 0;
  *value = 0;
  char* readHead = *ptr;

  // Fast path for the first 19 digits
  char* fastPathEnd = *ptr + 19;
  while (readHead < fastPathEnd) {
    if (readHead >= end) {
      return false;  // End of string reached
    }

    unsigned nextDigit = (unsigned)(*readHead - '0');
    if (nextDigit > 9) {
      break;  // Invalid character found
    }

    *value = (10 * *value) + nextDigit;
    readHead++;
  }

  // Parse remaining digits, checking for overflow
  while (readHead < end) {
    unsigned nextDigit = (unsigned)(*readHead - '0');
    if (nextDigit > 9) {
      break;  // Invalid character found
    }

    // Check for overflow based on UINT64_MAX (18446744073709551615)
    if ((*value == 1844674407370955161UL && ((int)nextDigit > 5)) ||
        (*value > 1844674407370955161UL)) {
      ThrowIntegerOverflow(*ptr, (size_t)(readHead - *ptr));
    }

    *value = (10 * *value) + nextDigit;
    readHead++;
  }

  // Update the pointer to the new location
  *bytes_read = (size_t)(readHead - *ptr);
  *ptr = readHead;

  return (*bytes_read > 0);  // Return true if any digits were read
}

/*
 * Helper function. Tries to read a RESP a signed length header from the given
 * ASCII-encoded RESP string and, if successful, moves the given ptr to the end
 * of the length header. Returns true if a length header was successfully read
 * and false otherwise.
 */
bool TryReadSignedLengthHeader(int* length, char** ptr, char* end,
                               bool isArray) {
  *length = -1;
  if (*ptr + 3 > end) {
    return false;  // Not enough space for the header
  }

  char* readHead = *ptr + 1;
  bool negative = (*readHead == '-');

  // String length headers must start with a '$', array headers with '*'
  if (**ptr != (isArray ? '*' : '$')) {
    ThrowUnexpectedToken(**ptr);
    return false;
  }

  // Special case: "$-1" (NULL value)
  if (negative) {
    if (readHead + 4 > end) {
      return false;
    }

    // Compare "$-1\r\n" directly as bytes
    if (memcmp(readHead, "-1\r\n", 4) == 0) {
      *ptr = readHead + 4;
      return true;
    }
    readHead++;  // Skip the '-'
  }

  // Parse the length (unsigned)
  uint64_t value;
  size_t digits_read = 0;
  if (!TryReadUint64(&readHead, end, &value, &digits_read)) {
    return false;  // Failed to parse number
  }

  if (digits_read == 0) {
    ThrowUnexpectedToken(*readHead);
    return false;
  }

  // Validate the length
  if (value > INT_MAX || (negative && value > ((uint64_t)INT_MAX + 1))) {
    ThrowUnexpectedToken(*(readHead - digits_read));
    return false;
  }

  // Convert to signed value
  *length = negative ? -(int)value : (int)value;

  // Ensure terminator is received ("\r\n")
  *ptr = readHead + 2;
  if (*ptr > end) {
    return false;
  }

  // Check for "\r\n" (CRLF) at the end of the header
  if (*(unsigned short*)readHead != 0x0A0D) {  // 0x0A0D is "\r\n" in hex
    ThrowUnexpectedToken(**ptr);
    return false;
  }

  return true;
}

/*
 * Helper function. Tries to read a RESP length header from the given
 * ASCII-encoded RESP string and, if successful, moves the given ptr to the end
 * of the length header. Returns true if a length header was successfully read
 * and false otherwise.
 */
bool TryReadUnsignedLengthHeader(int* length, char** ptr, char* end,
                                 bool isArray) {
  *length = -1;
  if (*ptr + 3 > end) {
    return false;
  }

  char* readHead = *ptr + 1;
  bool negative = (*readHead == '-');

  if (negative) {
    return false;
  }

  return TryReadSignedLengthHeader(length, ptr, end, isArray);
}

/*
 * Helper function. Read the next argument from the input buffer. Return true on
 * success and false otherwise.
 */
bool ParseStateRead(int i, char** ptr, char* end, ArgSlice* buffer) {
  ArgSlice* slice = &buffer[i];

  // Parse RESP string header (TryReadUnsignedLengthHeader equivalent in C)
  if (!TryReadUnsignedLengthHeader(&(slice->length), ptr, end, false)) {
    return false;
  }

  slice->ptr = *ptr;  // Store the pointer to the argument

  // Parse content: ensure that input contains key + '\r\n'
  *ptr += slice->length + 2;
  if (*ptr > end) {
    return false;
  }

  // Check for \r\n at the end of the content
  if (*(unsigned short*)(*ptr - 2) != 0x0A0D) {  // "\r\n" in hex
    return false;
  }

  return true;
}

/*
 * Helper function. Parses the RespCommand from the given input buffer in
 * session and returns it. session: Current RespServerSession being parsed.
 * success: Set to 1 if successfully parsed RespCommand or 0 otherwise.
 */
RespCommand ParseCommand(RespServerSession* session, int* success) {
  RespCommand cmd = RESP_COMMAND_NONE;

  int count = -1;  // readHead has not been advanced yet
  *success = 1;    // Assume success initially
  session->endReadHead = session->readHead;

  // Attempt fast parsing for common operations
  cmd = FastParseCommand(session, &count);

  // currently skipping ArrayParseCommand

  // If no command is found, set success to false and return RESP_COMMAND_NONE
  if (cmd == RESP_COMMAND_NONE) {
    *success = 0;
    return cmd;
  }

  // set up parse state
  InitializeParseStateCount(&session->parseState, count);

  char* ptr = session->recvBufferPtr + session->readHead;

  for (int i = 0; i < count; i++) {
    if (!ParseStateRead(i, &ptr, session->recvBufferPtr + session->bytesRead,
                        (&session->parseState)->buffer)) {
      *success = 0;
      return RESP_COMMAND_NONE;
    }
  }

  session->endReadHead = (int)(ptr - session->recvBufferPtr);

  return cmd;
}

/*
 * Adds ClientRequest to the client request queue. Return 0 on sucess and -1
 * otherwise.
 */
int ProcessBasicCommands(RespServerSession* session, RespCommand cmd) {
  int parameter_count = (&session->parseState)->count;
  ClientRequest req;

  // Insert request to the client request queue through the Prefetcher object
  if (parameter_count > 0 && (&session->parseState)->buffer != NULL) {
    // print all arguments for debugging purposes
    // for (int i = 0; i < parameter_count; i++) {
    //   printf("%.*s\n", (&session->parseState)->buffer[i].length,
    //   (&session->parseState)->buffer[i].ptr);
    //}

    req.type = cmd;

    // Allocate memory for key string (ensure null termination) - first argument
    // is the key
    req.id = (char*)malloc((&session->parseState)->buffer[0].length + 1);
    if (req.id != NULL) {
      strncpy(req.id, (&session->parseState)->buffer[0].ptr,
              (&session->parseState)->buffer[0].length);
      req.id[(&session->parseState)->buffer[0].length] = '\0';
    }

    // add to client request queue
    if ( true /*PassClientRequest(req)*/) {
      return 0;
    } else {
      fprintf(stderr, "Failed to add client request to queue.\n");
      return -1;
    }
  } else {
    req.id = NULL;
    return -1;
  }
}

/*
 * Helper function. Process messages for the session.
 * session: Current RespServerSession being parsed.
 */
void ProcessMessages(RespServerSession* session) {
  int _origReadHead = session->readHead;

  while (session->bytesRead - session->readHead >= 4) {
    // Parse the command and check if it's fully received
    int success = 0;
    RespCommand cmd = ParseCommand(session, &success);

    if (!success) {
      // command not fully received, reset addresses and break out
      session->endReadHead = session->readHead = _origReadHead;
      break;
    }

    // Directly process commands without ACL or script checks
    ProcessBasicCommands(session, cmd);

    // Advance read head to process the next command
    _origReadHead = session->readHead = session->endReadHead;

    // Metrics
    session->opCount++;
  }

  // Send response if there's data to send here
}

/*
 * Consume the incoming message and return the number of bytes successfully read
 * from the buffer. session: Current RespServerSession being parsed. reqBuffer:
 * The input buffer that the messages are being read from. bytesReceived: The
 * number of bytes received to be parsed in the reqBuffer.
 */
int TryConsumeMessages(RespServerSession* session, char* reqBuffer,
                       int bytesReceived) {
  session->bytesRead = bytesReceived;
  session->readHead = 0;  // reading from beginning of packet

  if (setjmp(session->exceptionBuffer) == 0) {
    session->recvBufferPtr = reqBuffer;

    ProcessMessages(session);

    session->recvBufferPtr = NULL;
  } else {
    // Exception handling
    HandleRespParsingException(session, "Parsing error occurred");
  }
  // return number of bytes successfully read from buffer
  return session->readHead;
}

}  // namespace

struct Request {
  uint8_t type;
  uint64_t key;
  bool local;
}__attribute__((packed));

void ProcessPacketMsg(char* msg, int msg_len, uint64_t& num_packets) {
  uint64_t num_msgs = msg_len / 10;
  num_packets += msg_len;

  // for (int i = 0; i < num_msgs; i++) {
    // Request* req = (Request*)(msg + i * 10);
    // std::cout << "type: " << req->type << ", key: " << req->key << ", local: " << req->local << '\n';
  // }
  // RespServerSession session;
  // memset(&session, 0, sizeof(session));

  // InitializeRespServerSession(&session);
  // TryConsumeMessages(&session, msg, msg_len);

  // // Clean up
  // FreeRespServerSession(&session);
}
