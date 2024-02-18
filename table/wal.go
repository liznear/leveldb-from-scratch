package table

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// loggable is an interface to indicate that the object can be logged.
type loggable interface {
	read(io.Reader) error

	write(io.Writer) (int, error)

	sizeOnDisk() int
}

type kvLog struct {
	kv kv
}

func newKVLog(key string, value []byte) *kvLog {
	return &kvLog{
		kv: newKV(key, value),
	}
}

func newDeletedKVLog(key string) *kvLog {
	return &kvLog{
		kv: newDeletedKey(key),
	}
}

func (l *kvLog) write(w io.Writer) (int, error) {
	return l.kv.write(w)
}

func (l *kvLog) read(r io.Reader) error {
	return l.kv.read(r)
}

func (l *kvLog) sizeOnDisk() int {
	return sizeOnDisk(l.kv.key.data, l.kv.value.data)
}

type versionLog struct {
	del []Gen
	add []Gen
	seq Seq
}

func (l *versionLog) debug() string {
	sb := strings.Builder{}
	sb.WriteString("Add: [")
	for i, a := range l.add {
		if i != 0 {
			sb.WriteByte(',')
		}
		_, _ = fmt.Fprintf(&sb, "%d", a)
	}
	sb.WriteString("]\n")
	sb.WriteString("Del: [")
	for i, d := range l.del {
		if i != 0 {
			sb.WriteByte(',')
		}
		_, _ = fmt.Fprintf(&sb, "%d", d)
	}
	sb.WriteString("]\n")
	_, _ = fmt.Fprintf(&sb, "Seq: %d\n", l.seq)
	return sb.String()
}

func (l *versionLog) write(w io.Writer) (int, error) {
	n := 0

	if err := binary.Write(w, binary.BigEndian, uint16(len(l.del))); err != nil {
		return n, err
	}
	n += 2
	for _, d := range l.del {
		if err := binary.Write(w, binary.BigEndian, d); err != nil {
			return n, err
		}
		n += 8
	}

	if err := binary.Write(w, binary.BigEndian, uint16(len(l.add))); err != nil {
		return n, err
	}
	n += 2
	for _, a := range l.add {
		if err := binary.Write(w, binary.BigEndian, a); err != nil {
			return n, err
		}
		n += 8
	}
	if err := binary.Write(w, binary.BigEndian, uint64(l.seq)); err != nil {
		return n, err
	}
	return n + 2, nil
}

func (l *versionLog) read(r io.Reader) error {
	var (
		dl uint16
		al uint16
	)

	if err := binary.Read(r, binary.BigEndian, &dl); err != nil {
		return err
	}
	l.del = make([]Gen, dl)
	for i := range l.del {
		if err := binary.Read(r, binary.BigEndian, &l.del[i]); err != nil {
			return err
		}
	}

	if err := binary.Read(r, binary.BigEndian, &al); err != nil {
		return err
	}
	l.add = make([]Gen, al)
	for i := range l.add {
		if err := binary.Read(r, binary.BigEndian, &l.add[i]); err != nil {
			return err
		}
	}

	var seq uint64
	if err := binary.Read(r, binary.BigEndian, &seq); err != nil {
		return err
	}
	l.seq = Seq(seq)
	return nil
}

func (l *versionLog) sizeOnDisk() int {
	return 2 + len(l.del)*8 + 2 + len(l.add)*8 + 8
}

type logWriter[T loggable] struct {
	w    io.WriteCloser
	sync func() error
}

func newKVLogWriter(seq Seq) (*logWriter[*kvLog], error) {
	_ = os.Remove(kvLogFile(seq))
	w, err := os.OpenFile(kvLogFile(seq), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("kv log writer: fail to open file: %w", err)
	}
	return &logWriter[*kvLog]{w, w.Sync}, nil
}

func newVersionLogWriter() (*logWriter[*versionLog], error) {
	w, err := os.OpenFile(versionLogFile(), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("version log writer: fail to open file: %w", err)
	}
	return &logWriter[*versionLog]{w, w.Sync}, nil
}

func (lw *logWriter[T]) Sync() error {
	return lw.sync()
}

func (lw *logWriter[T]) Write(log T) error {
	if _, err := log.write(lw.w); err != nil {
		return fmt.Errorf("log writer: fail to write log data: %w", err)
	}
	return nil
}

func (lw *logWriter[T]) Close() error {
	return lw.w.Close()
}

type logIter[T loggable] struct {
	r     *bufio.Reader
	close func() error
	n     int
}

func newKVLogIter(seq Seq) (*logIter[*kvLog], error) {
	r, err := os.Open(kvLogFile(seq))
	if err != nil {
		return nil, fmt.Errorf("kv log iter: fail to open file: %w", err)
	}
	return &logIter[*kvLog]{bufio.NewReader(r), r.Close, 0}, nil
}

func newVersionLogIter() (*logIter[*versionLog], error) {
	r, err := os.Open(versionLogFile())
	if err != nil {
		return nil, fmt.Errorf("version log iter: fail to open file: %w", err)
	}
	return &logIter[*versionLog]{bufio.NewReader(r), r.Close, 0}, nil
}

func (li *logIter[T]) Close() error {
	return li.close()
}

func (li *logIter[T]) Next() bool {
	_, err := li.r.Peek(1)
	return err == nil
}

func (li *logIter[T]) Read(v T) error {
	if err := v.read(li.r); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return &incompleteLogError{valid: li.n, remaining: li.r.Buffered()}
		}
		return fmt.Errorf("log reader: fail to read log data: %w", err)
	}
	li.n += v.sizeOnDisk()
	return nil
}

const walExtension = ".wal"

func kvLogFile(seq Seq) string {
	return fmt.Sprintf("%d%s", seq, walExtension)
}

func versionLogFile() string {
	return "version" + walExtension
}

type incompleteLogError struct {
	valid     int
	remaining int
	want      int
}

func (e *incompleteLogError) Error() string {
	return fmt.Sprintf("remaining %d bytes are incomplete", e.remaining)
}
