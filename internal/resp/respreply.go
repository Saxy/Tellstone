package resp

// respReply is the command-layer Reply encoder for RESP
type respReply struct{ out []byte }

func (r *respReply) OK()           { r.out = append(r.out, respOK...) }
func (r *respReply) Bulk(b []byte) { r.out = AppendBulk(r.out, b) }
func (r *respReply) Null()         { r.out = AppendNullBulk(r.out) }
func (r *respReply) Int(n int64)   { r.out = AppendInt(r.out, n) }
func (r *respReply) Denied(cmd string) {
	r.out = AppendError(r.out, "NOPERM no permission for '"+cmd+"' command on this key")
}
func (r *respReply) ErrorMsg(s string)    { r.out = AppendError(r.out, s) }
func (r *respReply) StorageErr(err error) { r.out = AppendError(r.out, "ERR "+err.Error()) }
