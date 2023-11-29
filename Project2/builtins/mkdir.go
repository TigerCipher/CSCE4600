package builtins

import (
	"os"
)

func MakeDirectory(args ...string) error {
	if len(args) != 1 {
		return ErrInvalidArgCount // defined in cd.go
	}

	dirName := args[0]
	err := os.Mkdir(dirName, 0755)
	if err != nil {
		return err
	}

	return nil
}
