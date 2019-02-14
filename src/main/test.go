package main
import (
	"fmt"
	"time"
)

func main () {
	ch := make(chan int)
	for i:=0;i< 10; i++{
		go func() {
			for w := range ch {
				fmt.Printf("%d\n",w)
			}
		}()
	}	
	go func() {
		for i :=0; i < 10; i++ {
			ch <- i
		}
		close(ch)
 	}()
	time.Sleep(10 * 1e9)
}