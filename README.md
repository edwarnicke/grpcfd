grpcfd is a simple set of tools to allow sending unix file descriptors over grpc when the grpc connection utilizes unix file sockets.

# Introduction

On a POSIX system (ie Linux), whenever a process opens a file, it had an integer 'file descriptor' (usually abbreviated fd) that acts
as a handle for that open file.

On a POSIX system, an open fd can be sent *over* an open socket connection to another process if and only if that socket connection
is over a unix file socket.  This allows the other process to access that open file itself, even if it normally couldn't *open* it because
the file was in a different mount namespace.

Since in POSIX almost everything is a file, this means shared memory, normal files, even sockets themselves can be passed along with a GRPC RPC call.

[GRPC](https://grpc.io/) is a popular high-performance, open source universal RPC framework with [excellent Go support](https://godoc.org/google.golang.org/grpc)

When using the GRPC Go implementation, you do not normally have access to the [net.Conn](https://golang.org/pkg/net/#Conn), and thus would not be able to
send file descriptors over a connection in use for GRPC.

# Client Setup

Simply wrap whatever normal transport credentials you use (nil is an acceptable value) using
```go grpcfd.TransportCredentials``` as a ```go grpc.WithTransportCredentials``` ```go grpc.DialOption``` when dialing:

```go
var creds credentials.TransportCredentials
cc, err := grpc.DialContext(ctx ,grpc.WithTransportCredentials(grpcfd.TransportCredentials(creds)))
```

Note: If your client already using PerRPCCredentials by default consider to use:

```go
var creds credentials.TransportCredentials
cc, err := grpc.DialContext(ctx, grpc.WithTransportCredentials(grpcfd.TransportCredentials(creds)), grpcfd.WithChainStreamInterceptor(), grpcfd.WithChainUnaryInterceptor())
```
This allows to grpcfd do not overwrite your grpc.PerRPCCredentials.


# Server Setup
Simply wrap whatever normal credentials you use (nil is an acceptable value) using
```go grpcfd.TransportCredentials``` as a ```go grpc.Creds``` ```go grpc.ServerOption``` when creating a ```go *grpc.Server```

```go
var creds credentials.TransportCredentials
server := grpc.NewServer(grpc.Creds(grpcfd.TransportCredentials(creds))
```

# Client To Server Example

## Client sending a file descriptor (fd) to Server

```go
// Wrap existing grpc.PerRPCCredentials using grpcfd.PerRPCredentials(...)
perRPCCredentials := grpcfd.PerRPCCredentials(perRPCCredentialsCanBeNilHere)
// Extract a grpcfd.FDSender from the rpcCredentials
sender, _ := grpcfd.FromPerRPCCredentials(perRPCCredentials)
// Send a file
errCh := sender.SendFilename(filename)
select {
case err := <-errCh:
    // If and error is immediately return... the file probably doesn't exist, handle that immediately
default:
    // Don't wait for any subsequent errors... they won't arrive till after we've sent the GRPC message
    // errCh will be closed after File is sent
}
client.MyRPC(ctx,arg,grpc.PerRPCCredentials(perRPCCredentials))
```

## Server receiving a file descriptor (fd) from a Client

```go
func (*myRPCImpl) MyRPC(ctx,arg) {
    // Extract a grpcfd.FDRecver from the ctx.  ok == false if one is not available
    recv, ok := grpcfd.FromContext(ctx)
    // Attempt to receive a filed by using a URL of the form inode://{{dev}}/{{ino}} where dev and ino are the values
    // from 
    fileCh, err := recv.RecvFileByURL(inodeURLStr)
}
```

# Server To Client Example

## Server sending a file descriptor (fd) to Client
```go
func (*myRPCImpl) MyRPC(ctx,arg) {
    // Extract a grpcfd.FDRecver from the ctx.  ok == false if one is not available
    sender, ok := grpcfd.FromContext(ctx)
    // Send a file 
    errCh := sender.SendFilename(filename)
    select {
    case err := <-errCh:
        // If and error is immediately return... the file probably doesn't exist, handle that immediately
    default:
        // Don't wait for any subsequent errors... they won't arrive till after we've sent the GRPC message
        // errCh will be closed after File is sent
    }
    ...
    return
}
```

## Client receiving file descriptor (fd) from Server
```go
// Wrap existing grpc.PerRPCCredentials using grpcfd.PerRPCredentials(...)
perRPCCredentials := grpcfd.PerRPCCredentials(perRPCCredentialsCanBeNilHere)
// Extract a grpcfd.FDRecver from the ctx.  ok == false if one is not available
recv, ok := grpcfd.FromContext(ctx)
client.MyRPC(ctx,arg,grpc.PerRPCCredentials(perRPCCredentials))
// Attempt to receive a filed by using a URL of the form inode://{{dev}}/{{ino}} where dev and ino are the values
// from 
fileCh, err := recv.RecvFileByURL(inodeURLStr)
```
