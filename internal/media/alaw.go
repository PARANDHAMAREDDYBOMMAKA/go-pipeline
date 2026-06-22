package media

func EncodeALaw(sample int16) uint8 {
	const aLawMax = 0x7FFF
	sign := uint8(0x55)
	s := int(sample)
	var compressed uint8
	if s >= 0 {
		sign = 0xD5
	} else {
		s = -s - 1
		if s < 0 {
			s = 0
		}
		sign = 0x55
	}
	if s > aLawMax {
		s = aLawMax
	}

	exponent := 7
	for mask := 0x4000; (s&mask) == 0 && exponent > 0; mask >>= 1 {
		exponent--
	}
	var mantissa int
	if exponent == 0 {
		mantissa = (s >> 4) & 0x0F
	} else {
		mantissa = (s >> (exponent + 3)) & 0x0F
	}
	compressed = uint8(exponent<<4) | uint8(mantissa)
	return compressed ^ sign
}

func DecodeALaw(a uint8) int16 {
	a ^= 0x55
	sign := a & 0x80
	exponent := int((a & 0x70) >> 4)
	mantissa := int(a & 0x0F)

	sample := (mantissa << 4) + 8
	if exponent != 0 {
		sample += 0x100
		sample <<= (exponent - 1)
	}
	if sign == 0 {
		sample = -sample
	}
	return int16(sample)
}

func EncodeALawFrame(samples []int16) []byte {
	out := make([]byte, len(samples))
	for i, s := range samples {
		out[i] = EncodeALaw(s)
	}
	return out
}

func DecodeALawFrame(data []byte) []int16 {
	out := make([]int16, len(data))
	for i, b := range data {
		out[i] = DecodeALaw(b)
	}
	return out
}
