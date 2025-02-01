package main

import (
	"fmt"
	"math/rand"
)

func main() {
	for i := 0; i < 10; i++ {
		var init_timeout int = (rand.Intn((3000-1500)/10+1) * 10) + 1500
		fmt.Println(init_timeout)
	}

}
