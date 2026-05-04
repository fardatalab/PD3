#pragma once

#include <cstdint>

// TODO: the ClientRequest that the network engine will pass over to the prefetcher
// should not be specific to RESP, and these types shouldn't be defined here.
// Fix this once we decided how the network engine <-> prefetcher interface should be.

/*
* Enum for RESP Commands
* Only contains partial commands that currently appear in the FastParseCommand function.
*/
typedef enum {
    RESP_COMMAND_NONE,

    // no arguments
    PING,
    EXEC,
    MULTI,
    ASKING,
    DISCARD,
    UNWATCH,
    READONLY,
    READWRITE,

    // fixed number of arguments
    GET,
    DEL,
    TTL,
    DUMP,
    INCR,
    PTTL,
    DECR,
    EXISTS,
    GETDEL,
    PERSIST,
    PFCOUNT,
    SET

    // Add more commands here
} RespCommand;

typedef char* RecordIdT;

// A parsed client network request
typedef struct {
    RespCommand type;
    RecordIdT id;
} ClientRequest;

struct HashmapClientRequest {
    uint64_t key;
    bool local;
};

using ClientRequestT = uint64_t;
