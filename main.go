package main

import (
	"Projekat/features"
	"Projekat/structures"
	"fmt"
)

func main() {
	//inicijalizacija
	wal := structures.CreateWAL(5, 1)
	mem := structures.CreateMemtable(5, 5, 0)
	wal.ReadAll(*mem)
	bloom := structures.ReadAll()
	cache := structures.CreateLRU(14)
	generation := 0
	//ovo sve treba iz config fajla da se cita

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
		// fmt.Scanln(&a)
		a = "3"

		switch a {
		case "x":
			return
		case "1":
			features.PUT(wal, mem, cache, &generation, *bloom)
		case "2":
			fmt.Println("AAA")
		case "3":
			value := features.GET(mem, cache, *bloom)
			if value != nil {
				fmt.Println("Pronadjen je i vrednost je ", string(value))
			} else {
				fmt.Println("Element sa trazenim kljucem nije pronadjen.")
			}
		default:
			fmt.Println("Pogrešan unos")
		}

		a = ""
	}
}
