package main

import (
	"encoding/gob"
	"fmt"
	"os"
)

func main() {
	bloomF := Create(30, 2)
	bloomF.Add("Laptop")
	bloomF.Add("Kompjuter")
	bloomF.Add("Telefon")
	bloomF.Add("Usisivac")
	bloomF.Add("Fen")
	bloomF.Add("Tastatura")

	fmt.Println("---Test strukture---")
	fmt.Println(bloomF.Query("Test123"))  // treba da ispise false
	fmt.Println(bloomF.Query("Usisivac")) // treba da ispise true

	//serijalizacija i deserijalizacija sa diska
	write_file, _ := os.Create("fajl.gob")
	write_file.Close()

	file, _ := os.OpenFile("fajl.gob", os.O_RDWR, 0666)
	defer file.Close()

	encoder := gob.NewEncoder(file)
	err := encoder.Encode(bloomF)
	if err != nil {
		fmt.Println(err)
	}

	decoder := gob.NewDecoder(file)
	var ucitano = new(BloomF)
	file.Seek(0, 0) // pozicioniranje na pocetak
	for {
		err = decoder.Decode(ucitano)
		if err != nil {
			break
		}
	}

	ucitano.hashF = bloomF.hashF //da budu iste hash funkcije
	fmt.Println("---Test serijalizacija/deserijalizacije---")
	fmt.Println(ucitano.Query("Test123"))  // treba da ispise false
	fmt.Println(ucitano.Query("Usisivac")) // treba da ispise true
}
