/*
Package network
Tellstone Secure TCP Networking Package
File: binaryreply.go
Description: The binary wire encoder backing the shared command layer. The
command handlers call exactly one Reply method per request; BinaryReply maps
those calls onto the binary protocol's frame/type pairs, with the payload
aliasing the frame buffer so encoding is allocation-free. The reply object is
pooled on the connection state and reset by runHandler before each request.

Authors:

	Maximilian Hagen
*/
package network

// BinaryReply implements command.Reply for the binary protocol
type BinaryReply struct {
	payload []byte
	mType   MessageType
	cause   error
}

// Reset clears the reply so the pooled value is ready for the next request.
func (r *BinaryReply) Reset() {
	r.payload = nil
	r.mType = 0
	r.cause = nil
}
func (r *BinaryReply) Result() ([]byte, MessageType) { return r.payload, r.mType }
func (r *BinaryReply) Cause() error                  { return r.cause }
func (r *BinaryReply) OK()                           { r.payload, r.mType = ResponseOK, MsgResponse }
func (r *BinaryReply) Bulk(b []byte)                 { r.payload, r.mType = b, MsgResponse }
func (r *BinaryReply) Null()                         { r.payload, r.mType = ResponseNotFound, MsgError }
func (r *BinaryReply) Int(n int64)                   { r.payload, r.mType = ResponseOK, MsgResponse }
func (r *BinaryReply) Denied(cmd string)             { r.payload, r.mType = ResponseNotAuthorized, MsgError }
func (r *BinaryReply) ErrorMsg(s string)             { r.payload, r.mType = []byte(s), MsgError }
func (r *BinaryReply) StorageErr(err error) {
	r.payload, r.mType = ResponseStorageFailure, MsgError
	r.cause = err
}
