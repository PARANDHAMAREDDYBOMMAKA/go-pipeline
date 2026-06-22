package media

import "math"

func AccumInto(acc []int32, frame []int16) {
	n := len(frame)
	if n > len(acc) {
		n = len(acc)
	}
	for i := 0; i < n; i++ {
		acc[i] += int32(frame[i])
	}
}

func SubFrom(acc []int32, frame []int16) {
	n := len(frame)
	if n > len(acc) {
		n = len(acc)
	}
	for i := 0; i < n; i++ {
		acc[i] -= int32(frame[i])
	}
}

func Limit(acc []int32) []int16 {
	out := make([]int16, len(acc))
	for i, v := range acc {
		if v > math.MaxInt16 {
			v = math.MaxInt16
		} else if v < math.MinInt16 {
			v = math.MinInt16
		}
		out[i] = int16(v)
	}
	return out
}

func RMS(frame []int16) float64 {
	if len(frame) == 0 {
		return 0
	}
	var sum float64
	for _, s := range frame {
		f := float64(s)
		sum += f * f
	}
	return math.Sqrt(sum / float64(len(frame)))
}
