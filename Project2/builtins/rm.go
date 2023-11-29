package builtins

import (
	"os"
)

func Remove(args ...string) error {
	if len(args) < 1 {
		return ErrInvalidArgCount
	}

	for _, path := range args {
		err := os.RemoveAll(path)
		if err != nil {
			return err
		}
	}

	return nil
}
