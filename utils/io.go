package utils

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// WriteWithUint32Length writes the length of the bytes as uint32 and then the bytes to the writer.
func WriteWithUint32Length(w io.Writer, bs []byte) (int, error) {
	if err := binary.Write(w, binary.BigEndian, uint32(len(bs))); err != nil {
		return 0, fmt.Errorf("fail to write length: %v", err)
	}
	if n, err := w.Write(bs); err != nil {
		return 4 + n, fmt.Errorf("fail to write bytes: %v", err)
	}
	return 4 + len(bs), nil
}

// ReadWithUint32Length reads the length of the bytes as uint32 first and then the bytes.
func ReadWithUint32Length(r io.Reader) ([]byte, error) {
	var l uint32
	if err := binary.Read(r, binary.BigEndian, &l); err != nil {
		// If we get EOF while reading the length, we should propagate the EOF error
		// directly so that the caller know there are no more data in the reader. It is
		// probably expected. Caller should decide how to handle it.
		//
		// EOF or ErrUnexpectedEOF after reading the length indicates that the data is
		// not long enough, and probably corrupted.
		if errors.Is(err, io.EOF) {
			return nil, err
		}
		return nil, fmt.Errorf("fail to read length: %v", err)
	}

	bs := make([]byte, l)

	if _, err := io.ReadFull(r, bs); err != nil {
		return nil, fmt.Errorf("fail to read bytes: %v", err)
	}
	return bs, nil
}
