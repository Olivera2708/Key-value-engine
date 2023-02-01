package features

import (
	"Projekat/structures"
	"encoding/binary"
	"fmt"
	"log"
	"os"
)

func LSM(sstype int, algorithm int, level int) int {
	if algorithm == 1 {
		if sstype == 1 {
			fmt.Println("sts")
			SizeTieredSingle(level)
		} else {
			fmt.Println("stm")
			SizeTieredMulti(level)
		}
	} else {
		if sstype == 1 {
			fmt.Println("ls")
			LeveledSingle(level)
		} else {
			fmt.Println("lm")
			LeveledMulti(level)
		}
	}
	return 0
}

func SizeTieredSingle(level int) {}
func LeveledSingle(level int)    {}
func LeveledMulti(level int)     {}

func SizeTieredMulti(level int) {
	var jos bool

	for lv := 0; lv < level; lv++ {
		generation := 0

		for i := 0; true; i++ {
			file, err := os.OpenFile("data/sstables/usertable-"+fmt.Sprint(lv+1)+"-"+fmt.Sprint(i)+"-data.db", os.O_RDONLY, 0666)
			if os.IsNotExist(err) {
				generation = i
				break
			}
			file.Close()
		}

		k := -1

		terminate_list := make([]int, 1)

		file1, err := os.OpenFile("data/sstables/usertable-"+fmt.Sprint(lv)+"-0-data.db", os.O_RDWR, 0666)
		if os.IsNotExist(err) {
			jos = false
			k = -1
			continue
		}

		terminate_list[0] = 0

		for jos = true; jos; {

			for i := 1; ; i++ {

				file2, err := os.OpenFile("data/sstables/usertable-"+fmt.Sprint(lv)+"-"+fmt.Sprint(i)+"-data.db", os.O_RDWR, 0666)
				if os.IsNotExist(err) {
					jos = false
					k = i - 1
					file2.Close()
					file1.Close()
					break
				}

				if jos {
					term := make([]int, 1)
					term[0] = i
					terminate_list = append(terminate_list, term...)
					//postoje dva fajla lv nivoa koje treba spojiti

					//novi fajl ima lvl: lv+1, indeks: i

					fmt.Println(generation)

					new_file, err := os.OpenFile("data/sstables/usertable-pomocna-"+fmt.Sprint(i)+"-data.db", os.O_CREATE|os.O_WRONLY, 0666)
					if err != nil {
						log.Fatal(err)
					}

					empty1 := false
					empty2 := false

					rec1, empty1 := structures.ReadNextRecord(file1)
					rec2, empty2 := structures.ReadNextRecord(file2)

					for {

						if empty1 {
							for !empty2 {
								if rec2["tombstone"][0] == 1 {
									continue
								}
								rec := append(rec2["crc"], rec2["timestamp"]...)
								rec = append(rec, rec2["tombstone"]...)
								rec = append(rec, rec2["key_size"]...)
								rec = append(rec, rec2["val_size"]...)
								rec = append(rec, rec2["key"]...)
								rec = append(rec, rec2["value"]...)
								new_file.Write(rec)

								rec2, empty2 = structures.ReadNextRecord(file2)
							}
							break

						} else if empty2 {
							for !empty1 {
								if rec1["tombstone"][0] == 1 {
									continue
								}
								rec := append(rec1["crc"], rec1["timestamp"]...)
								rec = append(rec, rec1["tombstone"]...)
								rec = append(rec, rec1["key_size"]...)
								rec = append(rec, rec1["val_size"]...)
								rec = append(rec, rec1["key"]...)
								rec = append(rec, rec1["value"]...)
								new_file.Write(rec)

								rec1, empty1 = structures.ReadNextRecord(file1)
							}
							break

						} else {
							if string(rec1["key"]) == string(rec2["key"]) { //jednaki kljucevi
								t1 := binary.LittleEndian.Uint64(rec1["timestamp"])
								t2 := binary.LittleEndian.Uint64(rec2["timestamp"])
								if t1 <= t2 && rec2["tombstone"][0] == 0 {
									rec := append(rec2["crc"], rec2["timestamp"]...)
									rec = append(rec, rec2["tombstone"]...)
									rec = append(rec, rec2["key_size"]...)
									rec = append(rec, rec2["val_size"]...)
									rec = append(rec, rec2["key"]...)
									rec = append(rec, rec2["value"]...)
									new_file.Write(rec)

								} else if t1 > t2 && rec1["tombstone"][0] == 0 {
									rec := append(rec1["crc"], rec1["timestamp"]...)
									rec = append(rec, rec1["tombstone"]...)
									rec = append(rec, rec1["key_size"]...)
									rec = append(rec, rec1["val_size"]...)
									rec = append(rec, rec1["key"]...)
									rec = append(rec, rec1["value"]...)
									new_file.Write(rec)
								}
								rec1, empty1 = structures.ReadNextRecord(file1)
								rec2, empty2 = structures.ReadNextRecord(file2)
								continue
							}

							if rec1["tombstone"][0] == 1 {
								rec1, empty1 = structures.ReadNextRecord(file1)
								continue
							}
							if rec2["tombstone"][0] == 1 {
								rec2, empty2 = structures.ReadNextRecord(file2)
								continue
							}

							if string(rec1["key"]) < string(rec2["key"]) {
								rec := append(rec1["crc"], rec1["timestamp"]...)
								rec = append(rec, rec1["tombstone"]...)
								rec = append(rec, rec1["key_size"]...)
								rec = append(rec, rec1["val_size"]...)
								rec = append(rec, rec1["key"]...)
								rec = append(rec, rec1["value"]...)
								new_file.Write(rec)

								rec1, empty1 = structures.ReadNextRecord(file1)
							} else if string(rec1["key"]) > string(rec2["key"]) {
								rec := append(rec2["crc"], rec2["timestamp"]...)
								rec = append(rec, rec2["tombstone"]...)
								rec = append(rec, rec2["key_size"]...)
								rec = append(rec, rec2["val_size"]...)
								rec = append(rec, rec2["key"]...)
								rec = append(rec, rec2["value"]...)
								new_file.Write(rec)

								rec2, empty2 = structures.ReadNextRecord(file2)
							}
						}

					}
					new_file.Close()
					err1 := file1.Close()
					if err1 != nil {
						log.Fatal(err1)
					}
					file2.Close()

					file1, err = os.OpenFile("data/sstables/usertable-pomocna-"+fmt.Sprint(i)+"-data.db", os.O_RDONLY, 0666)
					if err != nil {
						log.Fatal(err)
					}

				}

			}

		}

		os.Rename("data/sstables/usertable-pomocna-"+fmt.Sprint(k)+"-data.db",
			"data/sstables/usertable-"+fmt.Sprint(lv+1)+"-"+fmt.Sprint(generation)+"-data.db")

		if len(terminate_list) > 1 {
			for q := 0; q < len(terminate_list); q++ {
				err := os.Remove("data/sstables/usertable-" + fmt.Sprint(lv) + "-" + fmt.Sprint(terminate_list[q]) + "-data.db")
				if err != nil {
					log.Fatal(err)
				}
				/*
					os.Remove("data/sstables/usertable-" + fmt.Sprint(lv) + "-" + fmt.Sprint(terminate_list[q]) + "-filter.db")
					os.Remove("data/sstables/usertable-" + fmt.Sprint(lv) + "-" + fmt.Sprint(terminate_list[q]) + "-index.db")
					os.Remove("data/sstables/usertable-" + fmt.Sprint(lv) + "-" + fmt.Sprint(terminate_list[q]) + "-summary.db")
					os.Remove("data/sstables/usertable-" + fmt.Sprint(lv) + "-" + fmt.Sprint(terminate_list[q]) + "-TOC.txt")
				*/
			}

			for pom := 1; pom < k; pom++ {

				err := os.Remove("data/sstables/usertable-pomocna-" + fmt.Sprint(pom) + "-data.db")
				if os.IsNotExist(err) {
					break
				}
			}
		}

	}
}
