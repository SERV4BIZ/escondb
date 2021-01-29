package utility

// NumberByteToMb is calculate byte number to Megabyte number
func NumberByteToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
