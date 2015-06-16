package main

import (
	"crypto/md5"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/spf13/viper"
)

var name = flag.String("name", "World", "A File name to split")

var document = flag.String("joinFile", "joinFile.json", "This document describes which files to join")

var join bool

var useJSON bool

var useToml bool

var verbose bool

var fileToBeChunked string

// JoinFile = the description of the
type JoinFile struct {
	Name         string    `json:"name"`
	Parts        []string  `json:"parts"`
	Md5Sum       []byte    `json:"md5sum"`
	OrigSize     uint64    `json:"size"`
	CreationDate time.Time `json:"date"` // requires `import time`
}

func (j *JoinFile) setName(name string) {
	j.Name = name
}
func (j *JoinFile) setParts(parts []string) {
	j.Parts = parts
}
func (j *JoinFile) setMd5sum(md5sum []byte) {
	j.Md5Sum = md5sum
}
func (j *JoinFile) setSize(size uint64) {
	j.OrigSize = size
}
func (j *JoinFile) setDate(time time.Time) {
	j.CreationDate = time
}

func init() {
	flag.BoolVar(&join, "j", false, "Join Files as described in the accompany package.") //#3
	flag.BoolVar(&useJSON, "json", false, "Use json language.")                          //#3
	flag.BoolVar(&verbose, "verbose", false, "Verbose mode")                             //#3

}

func main() {

	flag.Parse()

	if join == true {
		joinFile := *document
		joiner(joinFile)

	} else {

		var jf = new(JoinFile)
		jf.setDate(time.Now())

		if *name == "World" {
			fmt.Println("name is empty, assigning the default name : ")
		} else {
			fileToBeChunked = *name
			fmt.Println("Name of file is : ", *name)
		}

		jf.setName(fileToBeChunked)

		if verbose == true {
			if hasher, err := md5er(fileToBeChunked); err != nil {
				fmt.Printf("Err: %v", err)
			} else {
				fmt.Printf("The hash of the file is : %x", hasher)
				jf.setMd5sum(hasher)
			}
		}

		file, err := os.Open(fileToBeChunked)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		defer file.Close()

		fileInfo, _ := file.Stat()

		var fileSize int64 = fileInfo.Size()

		jf.setSize(uint64(fileSize))

		fmt.Println("filesize", fileSize)

		const fileChunk = 1 * (1 << 30) // 1 GB, change this to your requirement

		fmt.Println("filechunck : ", fileChunk)

		// calculate total number of parts the file will be chunked into

		totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))

		fmt.Printf("Splitting to %d pieces.\n", totalPartsNum)

		partName := make([]string, totalPartsNum)

		for i := uint64(1); i <= totalPartsNum; i++ {
			fmt.Println("int64(i*fileChunk)", int64(i*fileChunk))
			fmt.Println("fileSize-int64(i*fileChunk)", fileSize-int64(i*fileChunk))
			var partSize int
			if i == totalPartsNum {
				partSize = int(fileSize - int64((i-1)*fileChunk))
			} else {
				partSize = fileChunk
			}
			partBuffer := make([]byte, partSize)
			file.Read(partBuffer)

			// write to disk
			fileName := fileToBeChunked + strconv.FormatUint(i, 10)
			_, err := os.Create(fileName)

			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			fmt.Println("i = : ", i)

			partName[i-1] = fileName

			fmt.Println("partName[i-1] : ", partName[i-1])
			// write/save buffer to disk
			ioutil.WriteFile(fileName, partBuffer, os.ModeAppend)

			fmt.Println("Split to : ", fileName)
		}

		jf.setParts(partName)

		writeOutput(*jf)
	}

}

func md5er(fileToBeMD5ed string) ([]byte, error) {

	var result []byte

	file, err := os.Open(fileToBeMD5ed)

	if err != nil {
		panic(err.Error())
	}

	defer file.Close()
	hash := md5.New()

	if _, err := io.Copy(hash, file); err != nil {
		return result, err
	}

	return hash.Sum(result), nil

}

func joiner(documenter string) (bool, error) {

	viper.SetConfigFile(*document)
	err := viper.ReadInConfig()

	if err != nil {

		fmt.Println("Config not found...")

	} else {

		name := viper.GetString("name")

		fmt.Println("Config found, name = ", name)
		//os.OpenFile(writeFile, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)
		f, _ := os.OpenFile(name, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)

		parts := viper.GetStringSlice("parts")

		for _, part := range parts {

			data, err := ioutil.ReadFile(part)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			f.Write(data)
		}

	}

	return false, nil
}

func writeOutput(jf JoinFile) (bool, error) {

	b, err := json.Marshal(jf)
	if err != nil {
		fmt.Println("error:", err)
	}

	jsonFile, err := os.Create(*document)

	if err != nil {
		fmt.Println(err)
	}
	defer jsonFile.Close()

	jsonFile.Write(b)
	jsonFile.Close()

	return false, nil
}
