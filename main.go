package main

import (
	"Projekat/features"
	"Projekat/structures"
	"fmt"
)

func main() {
	//inicijalizacija
	wal := structures.CreateWAL(5, 3)
	mem := structures.CreateMemtable(5, 5, 0)
	cache := structures.CreateLRU(14)
	generation := 0

	fmt.Println("-----------------------------------------------")
	fmt.Println("|                   OPCIJE                    |")
	fmt.Println("|                                             |")
	fmt.Println("| 1. Dodavanje (PUT)                          |")
	fmt.Println("| 2. Dobavljenje (GET)                        |")
	fmt.Println("| 3. Brisanje (DELETE)                        |")
	fmt.Println("| 4. Pretraga (LIST)                          |")
	fmt.Println("| 5. Traženje (RANGE SCAN)                    |")
	fmt.Println("|                                             |")
	fmt.Println("|                       Za izlaz ukucajte 'x' |")
	fmt.Println("-----------------------------------------------")

	var a string = ""

	for true {
		fmt.Print("\nIzaberite opciju -> ")
		fmt.Scanln(&a)

		switch a {
		case "x":
			return
		case "1":
			features.PUT(wal, mem, cache, &generation)
		case "2":
			fmt.Println("AAA")
		default:
			fmt.Println("Pogrešan unos")
		}
	}
}
