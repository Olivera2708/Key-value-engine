package structures

import "fmt"

func main() {
	cms := Create(0.1, 0.01)
	cms.Add("Laptop")
	cms.Add("Kompjuter")
	cms.Add("Telefon")
	cms.Add("Usisivac")
	cms.Add("Fen")
	cms.Add("Tastatura")

	fmt.Println("---Test strukture---")
	fmt.Println(cms.Query("Test123"))
	fmt.Println(cms.Query("Usisivac"))
	fmt.Println(cms.Query("Telefon"))
	fmt.Println(cms.Query("Usisivaci"))
}
