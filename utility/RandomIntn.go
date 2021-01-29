package utility

import (
	"math/rand"
	"time"
)

// RandomIntn is Random number 0 > n && n < length
func RandomIntn(length int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(length)
}
