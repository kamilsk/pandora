package pandora

import (
	"compress/gzip"
	"encoding/json"
	"strings"

	"github.com/DataDog/zstd"
	msgpack "github.com/ugorji/go/codec"
	"go.octolab.org/encoding"
	"go.octolab.org/errors"
	"go.octolab.org/io"
)

const (
	MSGPACK serializer  = "msgpack"
	JSON    serializer  = "json"
	GZIP    transformer = "gzip"
	ZSTD    transformer = "zstd"

	ErrInvalidFormat      errors.Message = "invalid format"
	ErrUnknownSerializer  errors.Message = "unknown serializer"
	ErrUnknownTransformer errors.Message = "unknown transformer"

	sep = "|"
)

type serializer string

func (s serializer) String() string { return string(s) }

func (s serializer) Encoder(writer io.Writer, tt ...transformer) (encoding.EncodeCloser, string) {
	format := s.pack(tt...)

	serialize, known := serializers[s]
	if !known {
		return nopSerializer(func(interface{}) error { return ErrUnknownSerializer }), format
	}

	var transform io.WriteCloserChain = func(output io.WriteCloser) (io.WriteCloser, error) { return output, nil }
	for _, t := range tt {
		transformer, known := transformers[t]
		if !known {
			return nopSerializer(func(interface{}) error { return ErrUnknownTransformer }), format
		}
		transform = transform.Chain(transformer.Output)
	}

	return serialize.Output(transform(io.ToWriteCloser(writer))), format
}

func (s serializer) pack(tt ...transformer) string {
	data := make([]string, 0, len(tt)+1)

	data = append(data, string(s))
	for _, t := range tt {
		data = append(data, string(t))
	}

	return strings.Join(data, sep)
}

func (s serializer) Decoder(reader io.Reader, format string) encoding.DecodeCloser {
	tt := s.unpack(format)

	serialize, known := serializers[s]
	if !known {
		return nopSerializer(func(interface{}) error { return ErrUnknownSerializer })
	}

	var transform io.ReadCloserChain = func(input io.ReadCloser) (io.ReadCloser, error) { return input, nil }
	for _, t := range tt {
		transformer, known := transformers[t]
		if !known {
			return nopSerializer(func(interface{}) error { return ErrUnknownTransformer })
		}
		transform = transform.Chain(transformer.Input)
	}

	return serialize.Input(transform(io.ToReadCloser(reader)))
}

func (s *serializer) unpack(format string) []transformer {
	data := strings.Split(format, sep)
	if len(data) == 0 {
		return nil
	}

	*s = serializer(data[0])

	tt := make([]transformer, 0, len(data[1:]))
	for _, t := range data[1:] {
		tt = append(tt, transformer(t))
	}

	return tt
}

type transformer string

func (t transformer) String() string { return string(t) }

//

type nopSerializer func(interface{}) error

func (fn nopSerializer) Decode(interface{}) error { return fn(nil) }
func (fn nopSerializer) Encode(interface{}) error { return fn(nil) }
func (fn nopSerializer) Close() error             { return nil }

//

var serializers = map[serializer]struct {
	Input  func(io.ReadCloser, error) encoding.DecodeCloser
	Output func(io.WriteCloser, error) encoding.EncodeCloser
}{
	MSGPACK: {
		Input: func(input io.ReadCloser, err error) encoding.DecodeCloser {
			if err != nil {
				return nopSerializer(func(interface{}) error { return err })
			}
			return encoding.ToDecodeCloser(msgpack.NewDecoder(input, new(msgpack.MsgpackHandle)), input)
		},
		Output: func(output io.WriteCloser, err error) encoding.EncodeCloser {
			if err != nil {
				return nopSerializer(func(interface{}) error { return err })
			}
			return encoding.ToEncodeCloser(msgpack.NewEncoder(output, new(msgpack.MsgpackHandle)), output)
		},
	},
	JSON: {
		Input: func(input io.ReadCloser, err error) encoding.DecodeCloser {
			if err != nil {
				return nopSerializer(func(interface{}) error { return err })
			}
			return encoding.ToDecodeCloser(json.NewDecoder(input), input)
		},
		Output: func(output io.WriteCloser, err error) encoding.EncodeCloser {
			if err != nil {
				return nopSerializer(func(interface{}) error { return err })
			}
			return encoding.ToEncodeCloser(json.NewEncoder(output), output)
		},
	},
}

var transformers = map[transformer]struct {
	Input  io.ReadCloserChain
	Output io.WriteCloserChain
}{
	GZIP: {
		Input: func(input io.ReadCloser) (io.ReadCloser, error) {
			decoder, err := gzip.NewReader(input)
			return io.CascadeReadCloser(decoder, input), err
		},
		Output: func(output io.WriteCloser) (io.WriteCloser, error) {
			encoder, err := gzip.NewWriterLevel(output, gzip.BestCompression)
			return io.CascadeWriteCloser(encoder, output), err
		},
	},
	ZSTD: {
		Input: func(input io.ReadCloser) (io.ReadCloser, error) {
			return io.CascadeReadCloser(zstd.NewReader(input), input), nil
		},
		Output: func(output io.WriteCloser) (io.WriteCloser, error) {
			return io.CascadeWriteCloser(zstd.NewWriterLevel(output, zstd.BestCompression), output), nil
		},
	},
}
