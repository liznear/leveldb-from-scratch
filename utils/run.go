package utils

type Runnable func() error

func ToRunnable3[T1, T2, T3 any](f func(T1, T2, T3) error, a T1, b T2, c T3) Runnable {
	return func() error {
		return f(a, b, c)
	}
}

func Run(rs ...Runnable) error {
	for _, r := range rs {
		if err := r(); err != nil {
			return err
		}
	}
	return nil
}
