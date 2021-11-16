## long-lived gRPC 예제

참고 : https://dev.bitolog.com/grpc-long-lived-streaming/

To compile the proto file, run the following command from the protos folder:

```bash
$ protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative longlived.proto
```

Note that this was tested on protoc version: libprotoc 3.6.1
