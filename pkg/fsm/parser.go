package fsm

import (
	"context"
	"errors"
	"unicode/utf8"

	"go-rec/pkg/pool"
)

const (
	MaxInputSize = 64 << 10
	scratchCap   = 128
	maxInt64U    = uint64(^uint64(0) >> 1)
)

var (
	ErrMalformed     = errors.New("fsm: malformed input")
	ErrInputTooLarge = errors.New("fsm: input too large")
	ErrValueTooLarge = errors.New("fsm: value too large")
)

type RerankRequest struct {
	SessionID    [36]byte
	SessionIDLen int
	VersionStamp int64
	Category     [64]byte
	CategoryLen  int
	Brand        [64]byte
	BrandLen     int
	Fallback     bool
}

func (r *RerankRequest) Reset() {
	for i := 0; i < r.SessionIDLen && i < len(r.SessionID); i++ {
		r.SessionID[i] = 0
	}
	for i := 0; i < r.CategoryLen && i < len(r.Category); i++ {
		r.Category[i] = 0
	}
	for i := 0; i < r.BrandLen && i < len(r.Brand); i++ {
		r.Brand[i] = 0
	}
	r.SessionIDLen = 0
	r.VersionStamp = 0
	r.CategoryLen = 0
	r.BrandLen = 0
	r.Fallback = false
}

func (r *RerankRequest) SessionIDString() string { return string(r.SessionID[:r.SessionIDLen]) }
func (r *RerankRequest) CategoryString() string  { return string(r.Category[:r.CategoryLen]) }
func (r *RerankRequest) BrandString() string     { return string(r.Brand[:r.BrandLen]) }

type parseState struct {
	i           int
	seenSession bool
	seenVersion bool
	seenSlots   bool
	seenCat     bool
	seenBr      bool
}

type Parser struct {
	statePool   *pool.MemoryPool[parseState]
	scratchPool *pool.ByteBufferPool
}

func NewParser() *Parser {
	return &Parser{
		statePool:   pool.NewMemoryPool(resetParseState),
		scratchPool: pool.NewByteBufferPool(scratchCap, scratchCap),
	}
}

func (p *Parser) Parse(ctx context.Context, input []byte, out *RerankRequest) error {
	if out == nil {
		return ErrMalformed
	}
	out.Reset()
	select {
	case <-ctx.Done():
		out.Fallback = true
		return ctx.Err()
	default:
	}
	if len(input) > MaxInputSize {
		out.Fallback = true
		return ErrInputTooLarge
	}
	if p == nil || p.statePool == nil || p.scratchPool == nil {
		out.Fallback = true
		return ErrMalformed
	}

	err := p.scratchPool.With(ctx, scratchCap, func(scratch []byte) error {
		return p.statePool.With(ctx, func(st *parseState) error {
			return st.parse(ctx, input, out, scratch[:0])
		})
	})
	if err != nil {
		out.Reset()
		out.Fallback = true
		return err
	}
	if out.CategoryLen == 0 || out.BrandLen == 0 {
		out.Fallback = true
	}
	return nil
}

func resetParseState(st *parseState) {
	st.i = 0
	st.seenSession = false
	st.seenVersion = false
	st.seenSlots = false
	st.seenCat = false
	st.seenBr = false
}

func (s *parseState) parse(ctx context.Context, input []byte, out *RerankRequest, scratch []byte) error {
	s.skipWS(input)
	if !s.consume(input, '{') {
		return ErrMalformed
	}
	s.skipWS(input)
	if s.consume(input, '}') {
		out.Fallback = true
		return ErrMalformed
	}
	for {
		if err := ctxErr(ctx); err != nil {
			return err
		}
		ks, ke, esc, err := s.scanString(input)
		if err != nil {
			return err
		}
		key := input[ks:ke]
		if esc {
			var ln int
			ln, err = unescapeIntoLimit(input, ks, ke, scratch[:cap(scratch)], true)
			if err != nil {
				if !errors.Is(err, ErrValueTooLarge) {
					return err
				}
				if err := validateEscapes(input, ks, ke); err != nil {
					return err
				}
			}
			if err == nil {
				key = scratch[:ln]
			} else {
				key = nil
			}
		}
		s.skipWS(input)
		if !s.consume(input, ':') {
			return ErrMalformed
		}
		s.skipWS(input)
		if bytesEqLit(key, "session_id") {
			if err := s.copyStringValue(input, out.SessionID[:], &out.SessionIDLen, scratch); err != nil {
				return err
			}
			if !validUUIDBytes(out.SessionID[:], out.SessionIDLen) {
				return ErrMalformed
			}
			s.seenSession = true
		} else if bytesEqLit(key, "version_stamp") {
			if err := s.parseVersion(input, out); err != nil {
				return err
			}
			s.seenVersion = true
		} else if bytesEqLit(key, "slots") {
			if err := s.parseSlots(ctx, input, out, scratch); err != nil {
				return err
			}
			s.seenSlots = true
		} else {
			if err := s.skipValue(ctx, input); err != nil {
				return err
			}
		}
		s.skipWS(input)
		if s.consume(input, '}') {
			break
		}
		if !s.consume(input, ',') {
			return ErrMalformed
		}
		s.skipWS(input)
	}
	s.skipWS(input)
	if s.i != len(input) {
		return ErrMalformed
	}
	if !s.seenSession || !s.seenVersion || !s.seenSlots {
		out.Fallback = true
		return ErrMalformed
	}
	return nil
}

func (s *parseState) parseSlots(ctx context.Context, input []byte, out *RerankRequest, scratch []byte) error {
	s.skipWS(input)
	if !s.consume(input, '{') {
		return ErrMalformed
	}
	s.skipWS(input)
	if s.consume(input, '}') {
		out.Fallback = true
		return nil
	}
	for {
		if err := ctxErr(ctx); err != nil {
			return err
		}
		ks, ke, esc, err := s.scanString(input)
		if err != nil {
			return err
		}
		key := input[ks:ke]
		if esc {
			var ln int
			ln, err = unescapeIntoLimit(input, ks, ke, scratch[:cap(scratch)], true)
			if err != nil {
				if !errors.Is(err, ErrValueTooLarge) {
					return err
				}
				if err := validateEscapes(input, ks, ke); err != nil {
					return err
				}
			}
			if err == nil {
				key = scratch[:ln]
			} else {
				key = nil
			}
		}
		s.skipWS(input)
		if !s.consume(input, ':') {
			return ErrMalformed
		}
		s.skipWS(input)
		if bytesEqLit(key, "category") {
			if err := s.copyStringValue(input, out.Category[:], &out.CategoryLen, scratch); err != nil {
				return err
			}
			s.seenCat = true
		} else if bytesEqLit(key, "brand") {
			if err := s.copyStringValue(input, out.Brand[:], &out.BrandLen, scratch); err != nil {
				return err
			}
			s.seenBr = true
		} else {
			if err := s.skipValue(ctx, input); err != nil {
				return err
			}
		}
		s.skipWS(input)
		if s.consume(input, '}') {
			break
		}
		if !s.consume(input, ',') {
			return ErrMalformed
		}
		s.skipWS(input)
	}
	if !s.seenCat || !s.seenBr || out.CategoryLen == 0 || out.BrandLen == 0 {
		out.Fallback = true
	}
	return nil
}

func (s *parseState) parseVersion(input []byte, out *RerankRequest) error {
	if s.i >= len(input) {
		return ErrMalformed
	}
	if input[s.i] == '"' {
		s.i++
		start := s.i
		var n int64
		if s.i >= len(input) || input[s.i] < '0' || input[s.i] > '9' {
			return ErrMalformed
		}
		if input[s.i] == '0' {
			s.i++
			if s.i < len(input) && input[s.i] >= '0' && input[s.i] <= '9' {
				return ErrMalformed
			}
		} else {
			var u uint64
			for s.i < len(input) && input[s.i] >= '0' && input[s.i] <= '9' {
				d := uint64(input[s.i] - '0')
				if u > (maxInt64U-d)/10 {
					return ErrMalformed
				}
				u = u*10 + d
				s.i++
			}
			n = int64(u)
		}
		if s.i == start || s.i >= len(input) || input[s.i] != '"' {
			return ErrMalformed
		}
		s.i++
		out.VersionStamp = n
		return nil
	}
	n, isInt, err := s.scanJSONInt(input)
	if err != nil {
		return err
	}
	if !isInt {
		return ErrMalformed
	}
	out.VersionStamp = n
	return nil
}

func (s *parseState) copyStringValue(input []byte, dst []byte, dstLen *int, scratch []byte) error {
	start, end, esc, err := s.scanString(input)
	if err != nil {
		return err
	}
	for i := 0; i < *dstLen && i < len(dst); i++ {
		dst[i] = 0
	}
	*dstLen = 0
	if !esc {
		ln := end - start
		if ln > len(dst) {
			return ErrValueTooLarge
		}
		copy(dst, input[start:end])
		*dstLen = ln
		return nil
	}
	ln, err := unescapeIntoLimit(input, start, end, scratch[:cap(scratch)], false)
	if err != nil {
		return err
	}
	if ln > len(dst) {
		return ErrValueTooLarge
	}
	copy(dst, scratch[:ln])
	*dstLen = ln
	return nil
}

func (s *parseState) scanString(input []byte) (int, int, bool, error) {
	if s.i >= len(input) || input[s.i] != '"' {
		return 0, 0, false, ErrMalformed
	}
	s.i++
	start := s.i
	esc := false
	for s.i < len(input) {
		c := input[s.i]
		if c == '"' {
			end := s.i
			s.i++
			return start, end, esc, nil
		}
		if c == '\\' {
			esc = true
			s.i++
			if s.i >= len(input) {
				return 0, 0, false, ErrMalformed
			}
		}
		if c < 0x20 {
			return 0, 0, false, ErrMalformed
		}
		s.i++
	}
	return 0, 0, false, ErrMalformed
}

func unescapeInto(input []byte, start, end int, dst []byte) (int, error) {
	return unescapeIntoLimit(input, start, end, dst, false)
}

func unescapeIntoLimit(input []byte, start, end int, dst []byte, allowOverflow bool) (int, error) {
	n := 0
	for i := start; i < end; i++ {
		c := input[i]
		if c != '\\' {
			if n >= len(dst) {
				if allowOverflow {
					return n, ErrValueTooLarge
				}
				return 0, ErrValueTooLarge
			}
			dst[n] = c
			n++
			continue
		}
		i++
		if i >= end {
			return 0, ErrMalformed
		}
		sc := input[i]
		switch sc {
		case '"', '\\', '/':
			if n >= len(dst) {
				if allowOverflow {
					return n, ErrValueTooLarge
				}
				return 0, ErrValueTooLarge
			}
			dst[n] = sc
			n++
		case 'b':
			if n >= len(dst) {
				if allowOverflow {
					return n, ErrValueTooLarge
				}
				return 0, ErrValueTooLarge
			}
			dst[n] = '\b'
			n++
		case 'f':
			if n >= len(dst) {
				if allowOverflow {
					return n, ErrValueTooLarge
				}
				return 0, ErrValueTooLarge
			}
			dst[n] = '\f'
			n++
		case 'n':
			if n >= len(dst) {
				if allowOverflow {
					return n, ErrValueTooLarge
				}
				return 0, ErrValueTooLarge
			}
			dst[n] = '\n'
			n++
		case 'r':
			if n >= len(dst) {
				if allowOverflow {
					return n, ErrValueTooLarge
				}
				return 0, ErrValueTooLarge
			}
			dst[n] = '\r'
			n++
		case 't':
			if n >= len(dst) {
				if allowOverflow {
					return n, ErrValueTooLarge
				}
				return 0, ErrValueTooLarge
			}
			dst[n] = '\t'
			n++
		case 'u':
			if i+4 >= end {
				return 0, ErrMalformed
			}
			cp := rune(0)
			for j := 1; j <= 4; j++ {
				h, ok := hexVal(input[i+j])
				if !ok {
					return 0, ErrMalformed
				}
				cp = cp<<4 | rune(h)
			}
			i += 4
			if cp >= 0xD800 && cp <= 0xDFFF {
				return 0, ErrMalformed
			}
			need := utf8.RuneLen(cp)
			if need < 0 {
				return 0, ErrMalformed
			}
			if n+need > len(dst) {
				if allowOverflow {
					return n, ErrValueTooLarge
				}
				return 0, ErrValueTooLarge
			}
			n += utf8.EncodeRune(dst[n:], cp)
		default:
			return 0, ErrMalformed
		}
	}
	return n, nil
}

func (s *parseState) skipValue(ctx context.Context, input []byte) error {
	return s.skipValueDepth(ctx, input, 0)
}

func validateEscapes(input []byte, start, end int) error {
	for i := start; i < end; i++ {
		if input[i] != '\\' {
			continue
		}
		i++
		if i >= end {
			return ErrMalformed
		}
		switch input[i] {
		case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
			continue
		case 'u':
			if i+4 >= end {
				return ErrMalformed
			}
			cp := rune(0)
			for j := 1; j <= 4; j++ {
				h, ok := hexVal(input[i+j])
				if !ok {
					return ErrMalformed
				}
				cp = cp<<4 | rune(h)
			}
			if cp >= 0xD800 && cp <= 0xDFFF {
				return ErrMalformed
			}
			i += 4
		default:
			return ErrMalformed
		}
	}
	return nil
}

func (s *parseState) skipValueDepth(ctx context.Context, input []byte, depth int) error {
	if depth > 64 {
		return ErrMalformed
	}
	if err := ctxErr(ctx); err != nil {
		return err
	}
	s.skipWS(input)
	if s.i >= len(input) {
		return ErrMalformed
	}
	switch input[s.i] {
	case '"':
		start, end, esc, err := s.scanString(input)
		if err != nil {
			return err
		}
		if esc {
			return validateEscapes(input, start, end)
		}
		return nil
	case '{':
		return s.skipObject(ctx, input, depth+1)
	case '[':
		return s.skipArray(ctx, input, depth+1)
	case 't':
		return s.consumeLit(input, "true")
	case 'f':
		return s.consumeLit(input, "false")
	case 'n':
		return s.consumeLit(input, "null")
	default:
		return s.skipNumber(input)
	}
}

func (s *parseState) skipObject(ctx context.Context, input []byte, depth int) error {
	if !s.consume(input, '{') {
		return ErrMalformed
	}
	s.skipWS(input)
	if s.consume(input, '}') {
		return nil
	}
	for {
		start, end, esc, err := s.scanString(input)
		if err != nil {
			return err
		}
		if esc {
			if err := validateEscapes(input, start, end); err != nil {
				return err
			}
		}
		s.skipWS(input)
		if !s.consume(input, ':') {
			return ErrMalformed
		}
		if err := s.skipValueDepth(ctx, input, depth); err != nil {
			return err
		}
		s.skipWS(input)
		if s.consume(input, '}') {
			return nil
		}
		if !s.consume(input, ',') {
			return ErrMalformed
		}
		s.skipWS(input)
		if s.i < len(input) && input[s.i] == '}' {
			return ErrMalformed
		}
	}
}

func (s *parseState) skipArray(ctx context.Context, input []byte, depth int) error {
	if !s.consume(input, '[') {
		return ErrMalformed
	}
	s.skipWS(input)
	if s.consume(input, ']') {
		return nil
	}
	for {
		if err := s.skipValueDepth(ctx, input, depth); err != nil {
			return err
		}
		s.skipWS(input)
		if s.consume(input, ']') {
			return nil
		}
		if !s.consume(input, ',') {
			return ErrMalformed
		}
		s.skipWS(input)
		if s.i < len(input) && input[s.i] == ']' {
			return ErrMalformed
		}
	}
}

func (s *parseState) consumeLit(input []byte, lit string) error {
	if len(input)-s.i < len(lit) {
		return ErrMalformed
	}
	for j := 0; j < len(lit); j++ {
		if input[s.i+j] != lit[j] {
			return ErrMalformed
		}
	}
	s.i += len(lit)
	return nil
}

func (s *parseState) skipNumber(input []byte) error {
	_, _, err := s.scanJSONInt(input)
	return err
}

func (s *parseState) scanJSONInt(input []byte) (int64, bool, error) {
	if s.i >= len(input) {
		return 0, false, ErrMalformed
	}
	neg := false
	if input[s.i] == '-' {
		neg = true
		s.i++
		if s.i >= len(input) {
			return 0, false, ErrMalformed
		}
	}
	var u uint64
	if input[s.i] == '0' {
		s.i++
		if s.i < len(input) && input[s.i] >= '0' && input[s.i] <= '9' {
			return 0, false, ErrMalformed
		}
	} else if input[s.i] >= '1' && input[s.i] <= '9' {
		limit := maxInt64U
		if neg {
			limit = maxInt64U + 1
		}
		for s.i < len(input) && input[s.i] >= '0' && input[s.i] <= '9' {
			d := uint64(input[s.i] - '0')
			if u > (limit-d)/10 {
				return 0, false, ErrMalformed
			}
			u = u*10 + d
			s.i++
		}
	} else {
		return 0, false, ErrMalformed
	}
	var n int64
	if neg && u == maxInt64U+1 {
		n = -int64(maxInt64U) - 1
	} else {
		n = int64(u)
		if neg {
			n = -n
		}
	}
	isInt := true
	if s.i < len(input) && input[s.i] == '.' {
		isInt = false
		s.i++
		start := s.i
		for s.i < len(input) && input[s.i] >= '0' && input[s.i] <= '9' {
			s.i++
		}
		if s.i == start {
			return 0, false, ErrMalformed
		}
	}
	if s.i < len(input) && (input[s.i] == 'e' || input[s.i] == 'E') {
		isInt = false
		s.i++
		if s.i < len(input) && (input[s.i] == '+' || input[s.i] == '-') {
			s.i++
		}
		start := s.i
		for s.i < len(input) && input[s.i] >= '0' && input[s.i] <= '9' {
			s.i++
		}
		if s.i == start {
			return 0, false, ErrMalformed
		}
	}
	return n, isInt, nil
}

func (s *parseState) skipWS(input []byte) {
	for s.i < len(input) {
		switch input[s.i] {
		case ' ', '\n', '\r', '\t':
			s.i++
		default:
			return
		}
	}
}

func (s *parseState) consume(input []byte, c byte) bool {
	if s.i < len(input) && input[s.i] == c {
		s.i++
		return true
	}
	return false
}

func bytesEqLit(b []byte, lit string) bool {
	if len(b) != len(lit) {
		return false
	}
	for i := 0; i < len(lit); i++ {
		if b[i] != lit[i] {
			return false
		}
	}
	return true
}

func isHex(c byte) bool {
	_, ok := hexVal(c)
	return ok
}

func hexVal(c byte) (byte, bool) {
	if c >= '0' && c <= '9' {
		return c - '0', true
	}
	if c >= 'a' && c <= 'f' {
		return c - 'a' + 10, true
	}
	if c >= 'A' && c <= 'F' {
		return c - 'A' + 10, true
	}
	return 0, false
}

func validUUIDBytes(b []byte, n int) bool {
	if n != 36 || len(b) < 36 {
		return false
	}
	for i := 0; i < 36; i++ {
		switch i {
		case 8, 13, 18, 23:
			if b[i] != '-' {
				return false
			}
		default:
			if !isHex(b[i]) {
				return false
			}
		}
	}
	return true
}

func ctxErr(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
