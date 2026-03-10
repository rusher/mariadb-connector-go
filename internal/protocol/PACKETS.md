# Protocol Packet Structure

This directory contains the MariaDB/MySQL protocol packet implementations, organized similarly to the MariaDB Java connector.

## Directory Structure

```
protocol/
├── message.go              # Base interfaces (ServerMessage, ClientMessage, Completion)
├── packet.go               # PacketReader and PacketWriter
├── constants.go            # Protocol constants and capability flags
├── server/                 # Server-to-client packets
│   ├── ok_packet.go       # OK packet (0x00 or 0xFE)
│   ├── eof_packet.go      # EOF packet (0xFE, deprecated with CLIENT_DEPRECATE_EOF)
│   ├── error_packet.go    # Error packet (0xFF)
│   └── ...                # Other server packets (handshake, column definition, etc.)
└── client/                 # Client-to-server packets
    └── ...                # Query, prepare, execute, etc.
```

## Design Pattern

Based on the MariaDB Java connector's message structure:
- **ServerMessage**: Interface for all server-to-client packets
- **ClientMessage**: Interface for all client-to-server packets
- **Completion**: Interface for command completion (OK/Error packets)

## Server Packets

### OkPacket (`server/ok_packet.go`)
Represents successful command completion.

**Format:**
- Header: 0x00 (or 0xFE when used as EOF with CLIENT_DEPRECATE_EOF)
- Affected rows: length-encoded integer
- Last insert ID: length-encoded integer
- Server status: 2 bytes
- Warning count: 2 bytes
- Info: length-encoded string (optional)
- Session state: length-encoded string (optional, with CLIENT_SESSION_TRACK)

**Usage:**
```go
okPacket, err := server.ParseOkPacket(data, eofDeprecated)
affectedRows := okPacket.GetAffectedRows()
lastInsertId := okPacket.GetLastInsertId()
serverStatus := okPacket.GetServerStatus()
```

### EOFPacket (`server/eof_packet.go`)
Marks the end of a result set.

**Traditional Format (CLIENT_DEPRECATE_EOF not set):**
- Header: 0xFE
- Warning count: 2 bytes
- Server status: 2 bytes
- Length: < 8 bytes

**Deprecated Format (CLIENT_DEPRECATE_EOF set):**
- Uses OkPacket with 0xFE header
- Length: < 0xFFFFFF bytes

**Usage:**
```go
if server.IsEOFPacket(data, eofDeprecated) {
    eofPacket, err := server.ParseEOFPacket(data, eofDeprecated)
    serverStatus := eofPacket.GetServerStatus()
    warnings := eofPacket.GetWarningCount()
}
```

### ErrorPacket (`server/error_packet.go`)
Represents an error from the server.

**Format:**
- Header: 0xFF
- Error code: 2 bytes
- SQL state marker: '#' (1 byte, optional)
- SQL state: 5 bytes (if marker present)
- Error message: rest of packet

**Usage:**
```go
if server.IsErrorPacket(data) {
    errPacket, err := server.ParseErrorPacket(data)
    return errPacket // Implements error interface
}
```

## EOF Packet Handling

The EOF packet behavior depends on the `CLIENT_DEPRECATE_EOF` capability:

### Without CLIENT_DEPRECATE_EOF (Traditional)
- EOF is a dedicated packet type (0xFE header, length < 8)
- Format: header + warnings + server status
- Used to mark end of column definitions and end of result set

### With CLIENT_DEPRECATE_EOF (Modern)
- EOF is replaced by OK packet with 0xFE header
- Format: header + affected rows + insert id + server status + warnings
- Length < 0xFFFFFF (16MB)
- More consistent with OK packet structure

**Detection Logic:**
```go
func IsEOFPacket(data []byte, eofDeprecated bool) bool {
    if len(data) == 0 || data[0] != 0xfe {
        return false
    }
    
    if eofDeprecated {
        return len(data) < 0xffffff  // OK packet with 0xFE
    }
    
    return len(data) < 8  // Traditional EOF packet
}
```

## Adding New Packet Types

To add a new packet type:

1. **Create packet file** in `server/` or `client/` directory
2. **Implement interface**:
   - Server packets: implement `ServerMessage` interface
   - Client packets: implement `ClientMessage` interface
3. **Add parser function**: `ParseXXXPacket(data []byte) (*XXXPacket, error)`
4. **Add detection function** (if needed): `IsXXXPacket(data []byte) bool`

### Example Template

```go
package server

type MyPacket struct {
    sequence uint8
    // ... fields
}

func ParseMyPacket(data []byte) (*MyPacket, error) {
    if len(data) < minSize {
        return nil, protocol.ErrMalformedPacket
    }
    
    // Parse packet fields
    
    return &MyPacket{
        // ... initialize fields
    }, nil
}

func (p *MyPacket) GetSequence() uint8 {
    return p.sequence
}

func IsMyPacket(data []byte) bool {
    return len(data) > 0 && data[0] == HEADER_BYTE
}
```

## References

- [MariaDB Protocol Documentation](https://mariadb.com/kb/en/clientserver-protocol/)
- [OK Packet](https://mariadb.com/kb/en/ok_packet/)
- [EOF Packet](https://mariadb.com/kb/en/eof_packet/)
- [ERR Packet](https://mariadb.com/kb/en/err_packet/)
- MariaDB Java Connector: `org.mariadb.jdbc.message` package
