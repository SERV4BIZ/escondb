package utility

import (
	"os"
	"path/filepath"
)

// GetAppDir is get path directory program work
func GetAppDir() string {
	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	exPath, _ := filepath.Abs(filepath.Dir(ex))
	return exPath
}
