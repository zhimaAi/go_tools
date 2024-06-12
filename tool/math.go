package tool

import "math"

func MaxInt(argv ...int) (max int) {
	if len(argv) > 0 {
		max = argv[0]
		for _, v := range argv {
			if v > max {
				max = v
			}
		}
	}
	return
}

func MinInt(argv ...int) (min int) {
	if len(argv) > 0 {
		min = argv[0]
		for _, v := range argv {
			if v < min {
				min = v
			}
		}
	}
	return
}

func MaxFloat64(argv ...float64) (max float64) {
	if len(argv) > 0 {
		max = argv[0]
		for _, v := range argv {
			if v > max {
				max = v
			}
		}
	}
	return
}

func MinFloat64(argv ...float64) (min float64) {
	if len(argv) > 0 {
		min = argv[0]
		for _, v := range argv {
			if v < min {
				min = v
			}
		}
	}
	return
}

func PowerInt(a int, b int) (c int) {
	if b < 0 {
		return
	}
	if a == 2 {
		return 1 << uint(b)
	}
	c = 1
	for i := 0; i < b; i++ {
		c = c * a
	}
	return
}

func PowerFloat64(a float64, b int) (c float64) {
	if b < 0 {
		return
	}
	c = 1
	for i := 0; i < b; i++ {
		c = c * a
	}
	return
}

func Ceil(f float64) int {
	return int(math.Ceil(f))
}

func Floor(f float64) int {
	return int(math.Floor(f))
}

func Round(f float64) int {
	return int(math.Floor(f + 0.5))
}

func AbsInt(f int) int {
	return int(math.Abs(float64(f)))
}
