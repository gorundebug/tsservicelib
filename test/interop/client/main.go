package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	interopv1 "github.com/gorundebug/tsservicelib/test/interop/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type report struct {
	Status         string   `json:"status"`
	Methods        []string `json:"methods"`
	RecoveryStatus string   `json:"recoveryStatus"`
}

func main() {
	address := flag.String("address", "127.0.0.1:19212", "TypeScript gRPC server address")
	flag.Parse()
	if err := run(*address); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(address string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	defer conn.Close()
	client := interopv1.NewInteropServiceClient(conn)
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(
		"x-stream-id", "go-official-client-stream",
		"x-interop-header", "go-official-client",
		"baggage", "interop=go-official-client",
	))

	if err := unary(ctx, client); err != nil {
		return err
	}
	if err := clientStreaming(ctx, client); err != nil {
		return err
	}
	if err := serverStreaming(ctx, client); err != nil {
		return err
	}
	if err := bidiStreaming(ctx, client); err != nil {
		return err
	}
	if err := failureAndRecovery(ctx, client); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(report{
		Status: "pass",
		Methods: []string{
			"unary", "client-streaming", "server-streaming", "bidirectional-streaming",
		},
		RecoveryStatus: codes.OK.String(),
	})
}

func unary(ctx context.Context, client interopv1.InteropServiceClient) error {
	var header, trailer metadata.MD
	response, err := client.Unary(ctx, echo(1, "unary"), grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		return fmt.Errorf("unary status: %w", err)
	}
	if !proto.Equal(response, echo(1, "unary")) {
		return fmt.Errorf("unary payload = %v", response)
	}
	return verifyMetadata("unary", header, trailer)
}

func clientStreaming(ctx context.Context, client interopv1.InteropServiceClient) error {
	stream, err := client.ClientStreaming(ctx)
	if err != nil {
		return fmt.Errorf("client-streaming open: %w", err)
	}
	for _, value := range []*interopv1.Echo{echo(1, "client"), echo(2, "client"), echo(3, "client")} {
		if err := stream.Send(value); err != nil {
			return fmt.Errorf("client-streaming send: %w", err)
		}
	}
	response, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("client-streaming status: %w", err)
	}
	if !proto.Equal(response, echo(3, "client")) {
		return fmt.Errorf("client-streaming payload = %v", response)
	}
	header, err := stream.Header()
	if err != nil {
		return fmt.Errorf("client-streaming header: %w", err)
	}
	return verifyMetadata("client-streaming", header, stream.Trailer())
}

func serverStreaming(ctx context.Context, client interopv1.InteropServiceClient) error {
	stream, err := client.ServerStreaming(ctx, echo(10, "server"))
	if err != nil {
		return fmt.Errorf("server-streaming open: %w", err)
	}
	var values []int64
	for {
		response, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			return fmt.Errorf("server-streaming status: %w", recvErr)
		}
		values = append(values, response.GetValue())
	}
	if !equalInt64(values, []int64{10, 11, 12}) {
		return fmt.Errorf("server-streaming payloads = %v", values)
	}
	header, err := stream.Header()
	if err != nil {
		return fmt.Errorf("server-streaming header: %w", err)
	}
	return verifyMetadata("server-streaming", header, stream.Trailer())
}

func bidiStreaming(ctx context.Context, client interopv1.InteropServiceClient) error {
	stream, err := client.BidirectionalStreaming(ctx)
	if err != nil {
		return fmt.Errorf("bidi open: %w", err)
	}
	for _, value := range []*interopv1.Echo{echo(4, "bidi"), echo(5, "bidi")} {
		if err := stream.Send(value); err != nil {
			return fmt.Errorf("bidi send: %w", err)
		}
	}
	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("bidi close send: %w", err)
	}
	var values []int64
	for {
		response, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			return fmt.Errorf("bidi status: %w", recvErr)
		}
		values = append(values, response.GetValue())
	}
	if !equalInt64(values, []int64{4, 5}) {
		return fmt.Errorf("bidi payloads = %v", values)
	}
	header, err := stream.Header()
	if err != nil {
		return fmt.Errorf("bidi header: %w", err)
	}
	return verifyMetadata("bidi", header, stream.Trailer())
}

func failureAndRecovery(ctx context.Context, client interopv1.InteropServiceClient) error {
	_, err := client.Unary(ctx, echo(0, "fail"))
	if status.Code(err) != codes.Unknown {
		return fmt.Errorf("failure status = %s, want %s", status.Code(err), codes.Unknown)
	}
	response, err := client.Unary(ctx, echo(9, "recovery"))
	if err != nil {
		return fmt.Errorf("recovery status: %w", err)
	}
	if !proto.Equal(response, echo(9, "recovery")) {
		return fmt.Errorf("recovery payload = %v", response)
	}
	return nil
}

func verifyMetadata(method string, header, trailer metadata.MD) error {
	for _, values := range []metadata.MD{header, trailer} {
		if len(values.Get("x-stream-id")) != 0 || len(values.Get("x-interop-header")) != 0 || len(values.Get("baggage")) != 0 {
			return fmt.Errorf("%s response leaked request metadata: header=%v trailer=%v", method, header, trailer)
		}
	}
	return nil
}

func echo(value int64, text string) *interopv1.Echo {
	return &interopv1.Echo{Value: value, Text: text}
}

func equalInt64(left, right []int64) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
