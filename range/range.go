package ranges

import (
	"fmt"
	"strconv"
	"strings"
)

type HttpRange struct {
	Start, Length int64
}

// parseRange parses a Range header string as per RFC 2616.
func ParseRange(s string, size int64) ([]HttpRange, error) {
	if s == "" {
		return nil, nil // header not present
	}
	const b = "bytes="
	if !strings.HasPrefix(s, b) {
		return nil, fmt.Errorf("%s: invalid range, there is no prefix '%s'", s, b)
	}
	var ranges []HttpRange
	for _, ra := range strings.Split(s[len(b):], ",") {
		ra = strings.TrimSpace(ra)
		if ra == "" {
			continue
		}
		i := strings.Index(ra, "-")
		if i < 0 {
			return nil, fmt.Errorf("%s: invalid range, there is no '-' separator", ra)
		}
		start, end := strings.TrimSpace(ra[:i]), strings.TrimSpace(ra[i+1:])
		var r HttpRange
		if start == "" {
			// If no start is specified, end specifies the
			// range start relative to the end of the file.
			i, err := strconv.ParseInt(end, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("%s: invalid range, can not convert end part '%s' into decimal number", ra, end)
			}
			if i > size {
				i = size
			}
			r.Start = size - i
			r.Length = size - r.Start
		} else {
			i, err := strconv.ParseInt(start, 10, 64)
			if err != nil || i >= size || i < 0 {
				return nil, fmt.Errorf("%s: invalid range, can not convert start part '%s' into decimal number or too large",
					ra, start)
			}
			r.Start = i
			if end == "" {
				// If no end is specified, range extends to end of the file.
				r.Length = size - r.Start
			} else {
				i, err := strconv.ParseInt(end, 10, 64)
				if err != nil || r.Start > i {
					return nil, fmt.Errorf("%s: invalid range, can not convert end part '%s' into decimal number", ra, end)
				}
				if i >= size {
					i = size - 1
				}
				r.Length = i - r.Start + 1
			}
		}
		ranges = append(ranges, r)
	}
	return ranges, nil
}
