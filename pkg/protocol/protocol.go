package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"streaming-engine/pkg/types"
	"time"
)

// Protocol constants
const (
	ProtocolVersion = 1
	MagicByte       = 0x53534D45       // "SSME" - Streaming Service Message Engine
	HeaderSize      = 20               // Fixed header size in bytes
	MaxMessageSize  = 16 * 1024 * 1024 // 16MB max message size
)

// MessageType represents different types of protocol messages
type MessageType uint8

const (
	// Producer messages
	MessageTypeProduceRequest  MessageType = 1
	MessageTypeProduceResponse MessageType = 2

	// Consumer messages
	MessageTypeFetchRequest   MessageType = 3
	MessageTypeFetchResponse  MessageType = 4
	MessageTypeCommitRequest  MessageType = 5
	MessageTypeCommitResponse MessageType = 6

	// Metadata messages
	MessageTypeMetadataRequest  MessageType = 7
	MessageTypeMetadataResponse MessageType = 8

	// Heartbeat messages
	MessageTypeHeartbeatRequest  MessageType = 9
	MessageTypeHeartbeatResponse MessageType = 10

	// Raft messages
	MessageTypeRaftAppendEntries   MessageType = 11
	MessageTypeRaftRequestVote     MessageType = 12
	MessageTypeRaftInstallSnapshot MessageType = 13

	// Admin messages
	MessageTypeCreateTopicRequest  MessageType = 14
	MessageTypeCreateTopicResponse MessageType = 15
	MessageTypeDeleteTopicRequest  MessageType = 16
	MessageTypeDeleteTopicResponse MessageType = 17
)

// MessageHeader represents the protocol message header
type MessageHeader struct {
	Magic       uint32      `json:"magic"`
	Version     uint8       `json:"version"`
	MessageType MessageType `json:"message_type"`
	Flags       uint8       `json:"flags"`
	Length      uint32      `json:"length"`
	CRC32       uint32      `json:"crc32"`
	RequestID   uint32      `json:"request_id"`
}

// Message represents a complete protocol message
type Message struct {
	Header  MessageHeader `json:"header"`
	Payload []byte        `json:"payload"`
}

// ProduceRequest represents a produce request
type ProduceRequest struct {
	ClientID    string                                `json:"client_id"`
	Acks        int32                                 `json:"acks"`
	Timeout     int32                                 `json:"timeout"`
	TopicData   map[string]map[int32][]*types.Message `json:"topic_data"` // topic -> partition -> messages
	Compression types.CompressionType                 `json:"compression"`
}

// ProduceResponse represents a produce response
type ProduceResponse struct {
	Responses map[string]map[int32]*PartitionResponse `json:"responses"` // topic -> partition -> response
	Timestamp time.Time                               `json:"timestamp"`
}

// PartitionResponse represents a response for a specific partition
type PartitionResponse struct {
	ErrorCode      types.ErrorCode `json:"error_code"`
	BaseOffset     int64           `json:"base_offset"`
	LogAppendTime  time.Time       `json:"log_append_time"`
	LogStartOffset int64           `json:"log_start_offset"`
}

// FetchRequest represents a fetch request
type FetchRequest struct {
	ClientID       string                               `json:"client_id"`
	MaxWaitTime    int32                                `json:"max_wait_time"`
	MinBytes       int32                                `json:"min_bytes"`
	MaxBytes       int32                                `json:"max_bytes"`
	IsolationLevel int8                                 `json:"isolation_level"`
	TopicData      map[string]map[int32]*FetchPartition `json:"topic_data"` // topic -> partition -> fetch info
}

// FetchPartition represents fetch information for a partition
type FetchPartition struct {
	FetchOffset    int64 `json:"fetch_offset"`
	LogStartOffset int64 `json:"log_start_offset"`
	MaxBytes       int32 `json:"max_bytes"`
}

// FetchResponse represents a fetch response
type FetchResponse struct {
	ThrottleTime time.Duration                                `json:"throttle_time"`
	ErrorCode    types.ErrorCode                              `json:"error_code"`
	Responses    map[string]map[int32]*FetchResponsePartition `json:"responses"` // topic -> partition -> response
}

// FetchResponsePartition represents a fetch response for a specific partition
type FetchResponsePartition struct {
	ErrorCode        types.ErrorCode  `json:"error_code"`
	HighWatermark    int64            `json:"high_watermark"`
	LastStableOffset int64            `json:"last_stable_offset"`
	LogStartOffset   int64            `json:"log_start_offset"`
	Messages         []*types.Message `json:"messages"`
}

// CommitRequest represents an offset commit request
type CommitRequest struct {
	GroupID       string                                `json:"group_id"`
	GenerationID  int32                                 `json:"generation_id"`
	MemberID      string                                `json:"member_id"`
	RetentionTime int64                                 `json:"retention_time"`
	TopicData     map[string]map[int32]*CommitPartition `json:"topic_data"` // topic -> partition -> commit info
}

// CommitPartition represents commit information for a partition
type CommitPartition struct {
	Offset   int64  `json:"offset"`
	Metadata string `json:"metadata"`
}

// CommitResponse represents an offset commit response
type CommitResponse struct {
	ThrottleTime time.Duration                                 `json:"throttle_time"`
	Responses    map[string]map[int32]*CommitResponsePartition `json:"responses"` // topic -> partition -> response
}

// CommitResponsePartition represents a commit response for a specific partition
type CommitResponsePartition struct {
	ErrorCode types.ErrorCode `json:"error_code"`
}

// MetadataRequest represents a metadata request
type MetadataRequest struct {
	Topics                             []string `json:"topics"`
	AllowAutoTopicCreation             bool     `json:"allow_auto_topic_creation"`
	IncludeClusterAuthorizedOperations bool     `json:"include_cluster_authorized_operations"`
}

// MetadataResponse represents a metadata response
type MetadataResponse struct {
	ThrottleTime  time.Duration      `json:"throttle_time"`
	Brokers       []types.BrokerInfo `json:"brokers"`
	ClusterID     string             `json:"cluster_id"`
	ControllerID  types.NodeID       `json:"controller_id"`
	TopicMetadata []TopicMetadata    `json:"topic_metadata"`
}

// TopicMetadata represents metadata for a topic
type TopicMetadata struct {
	ErrorCode         types.ErrorCode     `json:"error_code"`
	Name              string              `json:"name"`
	IsInternal        bool                `json:"is_internal"`
	PartitionMetadata []PartitionMetadata `json:"partition_metadata"`
}

// PartitionMetadata represents metadata for a partition
type PartitionMetadata struct {
	ErrorCode       types.ErrorCode `json:"error_code"`
	PartitionIndex  int32           `json:"partition_index"`
	LeaderID        types.NodeID    `json:"leader_id"`
	ReplicaNodes    []types.NodeID  `json:"replica_nodes"`
	ISRNodes        []types.NodeID  `json:"isr_nodes"`
	OfflineReplicas []types.NodeID  `json:"offline_replicas"`
}

// HeartbeatRequest represents a heartbeat request
type HeartbeatRequest struct {
	GroupID      string `json:"group_id"`
	GenerationID int32  `json:"generation_id"`
	MemberID     string `json:"member_id"`
}

// HeartbeatResponse represents a heartbeat response
type HeartbeatResponse struct {
	ThrottleTime time.Duration   `json:"throttle_time"`
	ErrorCode    types.ErrorCode `json:"error_code"`
}

// Encoder provides methods to encode protocol messages
type Encoder struct {
	buf *bytes.Buffer
}

// NewEncoder creates a new protocol encoder
func NewEncoder() *Encoder {
	return &Encoder{
		buf: &bytes.Buffer{},
	}
}

// EncodeMessage encodes a complete protocol message
func (e *Encoder) EncodeMessage(msgType MessageType, payload []byte, requestID uint32) ([]byte, error) {
	e.buf.Reset()

	header := MessageHeader{
		Magic:       MagicByte,
		Version:     ProtocolVersion,
		MessageType: msgType,
		Flags:       0,
		Length:      uint32(len(payload)),
		RequestID:   requestID,
	}

	// Calculate CRC32 of payload
	header.CRC32 = crc32.ChecksumIEEE(payload)

	// Encode header
	if err := binary.Write(e.buf, binary.BigEndian, header); err != nil {
		return nil, fmt.Errorf("failed to encode header: %w", err)
	}

	// Append payload
	if _, err := e.buf.Write(payload); err != nil {
		return nil, fmt.Errorf("failed to encode payload: %w", err)
	}

	return e.buf.Bytes(), nil
}

// EncodeProduceRequest encodes a produce request
func (e *Encoder) EncodeProduceRequest(req *ProduceRequest, requestID uint32) ([]byte, error) {
	payload, err := e.encodeProduceRequestPayload(req)
	if err != nil {
		return nil, err
	}
	return e.EncodeMessage(MessageTypeProduceRequest, payload, requestID)
}

// EncodeFetchRequest encodes a fetch request
func (e *Encoder) EncodeFetchRequest(req *FetchRequest, requestID uint32) ([]byte, error) {
	payload, err := e.encodeFetchRequestPayload(req)
	if err != nil {
		return nil, err
	}
	return e.EncodeMessage(MessageTypeFetchRequest, payload, requestID)
}

// Decoder provides methods to decode protocol messages
type Decoder struct {
	buf *bytes.Buffer
}

// NewDecoder creates a new protocol decoder
func NewDecoder() *Decoder {
	return &Decoder{
		buf: &bytes.Buffer{},
	}
}

// DecodeMessage decodes a complete protocol message
func (d *Decoder) DecodeMessage(data []byte) (*Message, error) {
	if len(data) < HeaderSize {
		return nil, fmt.Errorf("message too short: %d bytes", len(data))
	}

	d.buf.Reset()
	d.buf.Write(data)

	var header MessageHeader
	if err := binary.Read(d.buf, binary.BigEndian, &header); err != nil {
		return nil, fmt.Errorf("failed to decode header: %w", err)
	}

	// Validate magic byte
	if header.Magic != MagicByte {
		return nil, fmt.Errorf("invalid magic byte: 0x%x", header.Magic)
	}

	// Validate version
	if header.Version != ProtocolVersion {
		return nil, fmt.Errorf("unsupported protocol version: %d", header.Version)
	}

	// Validate message length
	if int(header.Length) != len(data)-HeaderSize {
		return nil, fmt.Errorf("invalid message length: expected %d, got %d",
			header.Length, len(data)-HeaderSize)
	}

	// Read payload
	payload := make([]byte, header.Length)
	if _, err := io.ReadFull(d.buf, payload); err != nil {
		return nil, fmt.Errorf("failed to read payload: %w", err)
	}

	// Verify CRC32
	actualCRC := crc32.ChecksumIEEE(payload)
	if actualCRC != header.CRC32 {
		return nil, fmt.Errorf("CRC32 mismatch: expected %x, got %x",
			header.CRC32, actualCRC)
	}

	return &Message{
		Header:  header,
		Payload: payload,
	}, nil
}

// DecodeProduceRequest decodes a produce request from payload
func (d *Decoder) DecodeProduceRequest(payload []byte) (*ProduceRequest, error) {
	d.buf.Reset()
	d.buf.Write(payload)

	// Implementation of produce request decoding
	req := &ProduceRequest{}
	// Detailed decoding logic would go here

	return req, nil
}

// DecodeFetchRequest decodes a fetch request from payload
func (d *Decoder) DecodeFetchRequest(payload []byte) (*FetchRequest, error) {
	d.buf.Reset()
	d.buf.Write(payload)

	// Implementation of fetch request decoding
	req := &FetchRequest{}
	// Detailed decoding logic would go here

	return req, nil
}

// Helper methods for encoding specific payload types
func (e *Encoder) encodeProduceRequestPayload(req *ProduceRequest) ([]byte, error) {
	e.buf.Reset()

	// Encode client ID
	if err := e.encodeString(req.ClientID); err != nil {
		return nil, err
	}

	// Encode acks
	if err := binary.Write(e.buf, binary.BigEndian, req.Acks); err != nil {
		return nil, err
	}

	// Encode timeout
	if err := binary.Write(e.buf, binary.BigEndian, req.Timeout); err != nil {
		return nil, err
	}

	// Encode topic count
	if err := binary.Write(e.buf, binary.BigEndian, int32(len(req.TopicData))); err != nil {
		return nil, err
	}

	// Encode topics and partitions
	for topic, partitions := range req.TopicData {
		if err := e.encodeString(topic); err != nil {
			return nil, err
		}

		if err := binary.Write(e.buf, binary.BigEndian, int32(len(partitions))); err != nil {
			return nil, err
		}

		for partitionID, messages := range partitions {
			if err := binary.Write(e.buf, binary.BigEndian, partitionID); err != nil {
				return nil, err
			}

			if err := e.encodeMessages(messages); err != nil {
				return nil, err
			}
		}
	}

	return e.buf.Bytes(), nil
}

func (e *Encoder) encodeFetchRequestPayload(req *FetchRequest) ([]byte, error) {
	e.buf.Reset()

	// Similar encoding logic for fetch requests
	// Implementation details would go here

	return e.buf.Bytes(), nil
}

func (e *Encoder) encodeString(s string) error {
	data := []byte(s)
	if err := binary.Write(e.buf, binary.BigEndian, int32(len(data))); err != nil {
		return err
	}
	_, err := e.buf.Write(data)
	return err
}

func (e *Encoder) encodeMessages(messages []*types.Message) error {
	if err := binary.Write(e.buf, binary.BigEndian, int32(len(messages))); err != nil {
		return err
	}

	for _, msg := range messages {
		if err := e.encodeMessage(msg); err != nil {
			return err
		}
	}

	return nil
}

func (e *Encoder) encodeMessage(msg *types.Message) error {
	// Encode key
	if err := e.encodeBytes(msg.Key); err != nil {
		return err
	}

	// Encode value
	if err := e.encodeBytes(msg.Value); err != nil {
		return err
	}

	// Encode timestamp
	timestamp := msg.Timestamp.UnixNano()
	if err := binary.Write(e.buf, binary.BigEndian, timestamp); err != nil {
		return err
	}

	// Encode headers
	if err := binary.Write(e.buf, binary.BigEndian, int32(len(msg.Headers))); err != nil {
		return err
	}

	for key, value := range msg.Headers {
		if err := e.encodeString(key); err != nil {
			return err
		}
		if err := e.encodeString(value); err != nil {
			return err
		}
	}

	return nil
}

func (e *Encoder) encodeBytes(data []byte) error {
	if err := binary.Write(e.buf, binary.BigEndian, int32(len(data))); err != nil {
		return err
	}
	_, err := e.buf.Write(data)
	return err
}
