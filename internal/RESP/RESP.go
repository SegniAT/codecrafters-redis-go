package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

var ErrInvalidValue = errors.New("invalid value")

type Type byte

const (
	SIMPLE_STRING Type = '+'
	SIMPLE_ERROR  Type = '-'
	INTEGER       Type = ':'
	BULK_STRING   Type = '$'
	ARRAY         Type = '*'
	NULL          Type = '_'
	BOOLEAN       Type = '#'
)

const (
	UNKNOWN_ALIAS       string = "Unknown"
	SIMPLE_STRING_ALIAS string = "SimpleString"
	SIMPLE_ERROR_ALIAS  string = "Error"
	INTEGER_ALIAS       string = "Integer"
	BULK_STRING_ALIAS   string = "BulkString"
	ARRAY_ALIAS         string = "Array"
	NULL_ALIAS          string = "Null"
	BOOLEAN_ALIAS       string = "Boolean"
)

func (t Type) String() string {
	switch t {
	default:
		return UNKNOWN_ALIAS
	case '+':
		return SIMPLE_STRING_ALIAS
	case '-':
		return SIMPLE_ERROR_ALIAS
	case ':':
		return INTEGER_ALIAS
	case '$':
		return BULK_STRING_ALIAS
	case '*':
		return ARRAY_ALIAS
	case '_':
		return NULL_ALIAS
	case '#':
		return BOOLEAN_ALIAS
	}
}

type Value struct {
	Typ          Type
	Simple_str   []byte
	Simple_err   []byte
	Integer      int64
	Num          int
	Bulk_str     []byte
	Bulk_str_err bool
	Bulk_str_rdb bool
	Array        []Value
	Null         bool
	Boolean      bool
}

func (v Value) String() string {
	switch v.Typ {
	case SIMPLE_STRING:
		return string(v.Simple_str)
	case SIMPLE_ERROR:
		return string(v.Simple_err)
	case INTEGER:
		return fmt.Sprintf("%d", v.Integer)
	case BULK_STRING:
		return string(v.Bulk_str)
	case ARRAY:
		ln := len(v.Array)
		arr := "[ "
		for ind, el := range v.Array {
			arr += el.String()
			if ind != ln-1 {
				arr += ", "
			}
		}
		arr += " ]"
		return arr
	case NULL:
		return ""
	case BOOLEAN:
		if v.Boolean {
			return "true"
		} else {
			return "false"
		}
	default:
		return "no string value"
	}
}

type Resp struct {
	reader *bufio.Reader
}

func NewRes(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

// read till "\r\n", and return without these bytes
func (r *Resp) readLine() (line []byte, n int, err error) {
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n += 1
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}

	return line[:len(line)-2], n, nil
}

func (r *Resp) readInteger() (x, n int, err error) {
	line, n, err := r.readLine()
	if err != nil {
		return 0, 0, err
	}

	// strconv.ParseInt handles both '+' and '-' as prefixes
	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}

	return int(i64), n, nil
}

func (r *Resp) Read() (Value, error) {
	_type, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch Type(_type) {
	case SIMPLE_STRING:
		return r.readSimpleString()
	case SIMPLE_ERROR:
		return r.readSimpleError()
	case INTEGER:
		return r.readIntegerValue()
	case BULK_STRING:
		return r.readBulkString()
	case ARRAY:
		return r.readArray()
	case NULL:
		return r.readNull()
	case BOOLEAN:
		return r.readBoolean()
	default:
		fmt.Printf("Unknown type: %v", string(_type))
		return Value{}, nil
	}
}

// +OK\r\n
func (r *Resp) readSimpleString() (Value, error) {
	v := Value{Typ: SIMPLE_STRING}
	line, _, err := r.readLine()
	if err != nil {
		return v, err
	}

	v.Simple_str = line

	return v, nil
}

// -Error message\r\n
func (r *Resp) readSimpleError() (Value, error) {
	v := Value{Typ: SIMPLE_ERROR}
	line, _, err := r.readLine()
	if err != nil {
		return v, err
	}

	v.Simple_err = line
	return v, nil
}

// :[<+|->]<value>\r\n (optional sign)
func (r *Resp) readIntegerValue() (Value, error) {
	v := Value{Typ: INTEGER}

	line, _, err := r.readLine()
	if err != nil {
		return v, err
	}

	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return v, err
	}

	v.Integer = i64

	return v, nil
}

// $<length>\r\n<data>\r\n
func (r *Resp) readBulkString() (Value, error) {
	v := Value{Typ: BULK_STRING}

	len, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	bulk := make([]byte, len)

	r.reader.Read(bulk)

	v.Bulk_str = bulk

	_, _, err = r.readLine()
	if err != nil {
		return v, err
	}

	return v, nil
}

// $<length>\r\n<data>
func (r *Resp) ReadRDB() (Value, error) {
	v := Value{Typ: BULK_STRING, Bulk_str_rdb: true}

	// read the first byte - $
	r.reader.ReadByte()

	len, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	bulk := make([]byte, len)
	readBytes, err := r.reader.Read(bulk)
	if err != nil {
		return v, err
	}

	if len != readBytes {
		return v, fmt.Errorf("mismatch: expected to read %d bytes, got %d", len, readBytes)
	}

	v.Bulk_str = bulk

	return v, nil
}

// *<number-of-elements>\r\n<element-1>...<element-n>
func (r *Resp) readArray() (Value, error) {
	v := Value{Typ: ARRAY, Array: make([]Value, 0)}

	len, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	for i := 0; i < len; i++ {
		val, err := r.Read()
		if err != nil {
			return v, err
		}

		v.Array = append(v.Array, val)
	}

	return v, nil
}

// _\r\n
func (r *Resp) readNull() (Value, error) {
	v := Value{Typ: NULL}
	v.Null = true

	_, _, err := r.readLine()
	if err != nil {
		return v, err
	}

	return v, nil
}

// #<t|f>\r\n
func (r *Resp) readBoolean() (Value, error) {
	v := Value{Typ: BOOLEAN}
	line, _, err := r.readLine()
	if err != nil {
		return v, err
	}

	if str := string(line); str == "t" {
		v.Boolean = true
	} else if str == "f" {
		v.Boolean = false
	} else {
		return v, ErrInvalidValue
	}

	return v, nil
}

// writing RESP
func (v Value) Marshal() []byte {
	switch v.Typ {
	case SIMPLE_STRING:
		return v.marshalSimpleString()
	case SIMPLE_ERROR:
		return v.marshalSimpleError()
	case INTEGER:
		return v.marshalInteger()
	case BULK_STRING:
		return v.marshalBulkString()
	case ARRAY:
		return v.MarshalArray()
	case NULL:
		v.marshalNull()
	case BOOLEAN:
		v.marshalBoolean()
	default:
		return []byte{}
	}
	return []byte{}
}

// +OK\r\n
func (v Value) marshalSimpleString() []byte {
	var bytes []byte
	bytes = append(bytes, byte(SIMPLE_STRING))
	bytes = append(bytes, v.Simple_str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

// -Error message\r\n
func (v Value) marshalSimpleError() []byte {
	var bytes []byte
	bytes = append(bytes, byte(SIMPLE_ERROR))
	bytes = append(bytes, v.Simple_err...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

// :[<+|->]<value>\r\n (optional sign)
func (v Value) marshalInteger() []byte {
	var bytes []byte
	bytes = append(bytes, byte(INTEGER))
	num := v.Integer
	if num < 0 {
		num = -num
		bytes = append(bytes, '-')
	}
	bytes = append(bytes, strconv.Itoa(int(v.Integer))...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

// $<length>\r\n<data>\r\n
func (v Value) marshalBulkString() []byte {

	// $-1\r\n
	var bytes []byte
	if v.Bulk_str_err {
		bytes = append(bytes, byte(BULK_STRING))
		bytes = append(bytes, '-', '1')
		bytes = append(bytes, '\r', '\n')
		return bytes
	}

	bytes = append(bytes, byte(BULK_STRING))
	bytes = append(bytes, strconv.Itoa(len(v.Bulk_str))...)
	bytes = append(bytes, '\r', '\n')

	bytes = append(bytes, v.Bulk_str...)
	if !v.Bulk_str_rdb {
		bytes = append(bytes, '\r', '\n')
	}

	return bytes
}

// *<number-of-elements>\r\n<element-1>...<element-n>
func (v Value) MarshalArray() []byte {
	length := len(v.Array)
	var bytes []byte
	bytes = append(bytes, byte(ARRAY))
	bytes = append(bytes, strconv.Itoa(length)...)
	bytes = append(bytes, '\r', '\n')

	for i := 0; i < length; i++ {
		bytes = append(bytes, v.Array[i].Marshal()...)
	}

	return bytes
}

// _\r\n
func (v Value) marshalNull() []byte {
	var bytes []byte
	bytes = append(bytes, byte(NULL))
	bytes = append(bytes, '\r', '\n')
	return bytes
}

// #<t|f>\r\n
func (v Value) marshalBoolean() []byte {
	var bytes []byte
	bytes = append(bytes, byte(BOOLEAN))
	if v.Boolean {
		bytes = append(bytes, 't')
	} else {
		bytes = append(bytes, 'f')
	}
	bytes = append(bytes, '\r', '\n')

	return bytes
}

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

func (w *Writer) Write(v Value) error {
	var bytes = v.Marshal()

	_, err := w.writer.Write(bytes)
	if err != nil {
		return err
	}

	return nil
}
