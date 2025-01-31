package test

import "fmt"

func main() {
	alive := make(map[string]string)
	conn := make(map[string]string)
	_, exists := alive["huh"]
	if exists != true {
		fmt.Println("Doesn't exist!")
	}
}
