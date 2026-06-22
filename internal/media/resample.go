package media

func Resample(in []int16, fromRate, toRate int) []int16 {
	if fromRate == toRate || len(in) == 0 {
		return in
	}
	outLen := len(in) * toRate / fromRate
	if outLen <= 0 {
		return nil
	}
	out := make([]int16, outLen)

	step := float64(fromRate) / float64(toRate)
	for i := 0; i < outLen; i++ {
		pos := float64(i) * step
		idx := int(pos)
		frac := pos - float64(idx)
		s0 := int(in[idx])
		s1 := s0
		if idx+1 < len(in) {
			s1 = int(in[idx+1])
		}
		out[i] = int16(float64(s0) + (float64(s1-s0) * frac))
	}
	return out
}

func ResampleFrame(p PCM, toRate int) PCM {
	p.Samples = Resample(p.Samples, p.SampleRate, toRate)
	p.SampleRate = toRate
	return p
}
