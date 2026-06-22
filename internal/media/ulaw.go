package media

const ulawBias = 0x84
const ulawClip = 32635

func EncodeULaw(sample int16) uint8 {
	sign := uint8(0)
	s := int(sample)
	if s < 0 {
		s = -s
		sign = 0x80
	}
	if s > ulawClip {
		s = ulawClip
	}
	s += ulawBias

	exponent := 7
	for mask := 0x4000; (s&mask) == 0 && exponent > 0; mask >>= 1 {
		exponent--
	}
	mantissa := (s >> (exponent + 3)) & 0x0F
	return ^(sign | uint8(exponent<<4) | uint8(mantissa))
}

func DecodeULaw(u uint8) int16 {
	u = ^u
	sign := u & 0x80
	exponent := (u >> 4) & 0x07
	mantissa := u & 0x0F
	sample := (int(mantissa) << 3) + ulawBias
	sample <<= exponent
	sample -= ulawBias
	if sign != 0 {
		sample = -sample
	}
	return int16(sample)
}

func EncodeULawFrame(samples []int16) []byte {
	out := make([]byte, len(samples))
	for i, s := range samples {
		out[i] = EncodeULaw(s)
	}
	return out
}

func DecodeULawFrame(data []byte) []int16 {
	out := make([]int16, len(data))
	for i, b := range data {
		out[i] = DecodeULaw(b)
	}
	return out
}
