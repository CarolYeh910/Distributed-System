package surfstore

import (
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"os"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	localMap, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Println(err)
	}
	changed := updataLocal(client, localMap)

	InfoMap := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&InfoMap)
	if err != nil {
		log.Println(err)
	}

	for fname, data := range InfoMap {
		local, ok := localMap[fname]

		if !ok || data.Version > local.Version {
			if !ok {
				localMap[fname] = &FileMetaData{}
			}
			download(client, fname, data, localMap)
		} else {
			if data.Version < local.Version {
				upload(client, fname, local)
			} else {
				_, ok := changed[fname]
				if ok {
					download(client, fname, data, localMap)
				}
			}
		}
	}

	for fname, data := range localMap {
		_, ok := InfoMap[fname]
		if !ok {
			upload(client, fname, data)
		}
	}
	WriteMetaFile(localMap, client.BaseDir)
}

func upload(client RPCClient, fname string, data *FileMetaData) {
	addrs := getAddrs(client, data.BlockHashList)
	path := ConcatPath(client.BaseDir, fname)

	f, err := os.Open(path)
	defer f.Close()

	if err == nil {
		var succ bool
		var i int
		size := client.BlockSize

		reader := bufio.NewReader(f)
		buf := make([]byte, size)
		result := bytes.NewBuffer(nil)

		for {
			n, err := reader.Read(buf) // read up to len(buf) bytes, maybe lesser
			if err != nil && err != io.EOF {
				log.Println(err)
			}
			if err == io.EOF {
				break
			}

			result.Write(buf[0:n])
			if result.Len() > size {
				bk := Block{BlockData: result.Next(size), BlockSize: int32(size)}
				err := client.PutBlock(&bk, addrs[i], &succ)
				if err != nil {
					log.Println(err)
				}
				i++
			}
		}
		if result.Len() > 0 {
			lsize := result.Len()
			bk := Block{BlockData: result.Next(lsize), BlockSize: int32(lsize)}
			err := client.PutBlock(&bk, addrs[i], &succ)
			if err != nil {
				log.Println(err)
			}
		}
	}

	var ver int32
	err = client.UpdateFile(data, &ver)
	if err != nil {
		log.Println(err)
	}
	if ver == -1 {
		log.Printf("Local file out of data")
	}
}

func download(client RPCClient, fname string, data *FileMetaData, localMap map[string]*FileMetaData) {
	hashes := data.BlockHashList
	path := ConcatPath(client.BaseDir, fname)

	var blocks []*Block
	deleted := false
	if len(hashes) == 1 && hashes[0] == "0" {
		deleted = true
	} else if len(hashes) != 1 || hashes[0] != "-1" {
		addrs := getAddrs(client, hashes)
		for i, hash := range hashes {
			bk := &Block{}
			err := client.GetBlock(hash, addrs[i], bk)
			if err != nil {
				log.Println(err)
			}
			blocks = append(blocks, bk)
		}
	}
	if deleted {
		os.Remove(path)
	} else {
		writeFile(path, blocks)
	}

	localMap[fname].Filename = fname
	localMap[fname].Version = data.Version
	localMap[fname].BlockHashList = data.BlockHashList
}

func getAddrs(client RPCClient, hashes []string) []string {
	var addrs []string
	bsMap := make(map[string][]string)
	err := client.GetBlockStoreMap(hashes, &bsMap)
	if err != nil {
		log.Println(err)
	}

	for _, hash := range hashes {
		for addr, hashlist := range bsMap {
			for _, val := range hashlist {
				if hash == val {
					addrs = append(addrs, addr)
				}
			}
		}
	}
	return addrs
}

func updataLocal(client RPCClient, localMap map[string]*FileMetaData) map[string]bool {
	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Println(err)
	}
	changed := make(map[string]bool)
	curFiles := make(map[string]bool)

	for _, f := range files {
		if f.Name() != "index.db" {
			path := ConcatPath(client.BaseDir, f.Name())
			hashlist := readFile(path, client.BlockSize)
			curFiles[f.Name()] = true

			data, ok := localMap[f.Name()]
			diff := true
			if ok {
				localList := data.BlockHashList
				if len(hashlist) == len(localList) {
					i := 0
					for ; i < len(hashlist); i++ {
						if hashlist[i] != localList[i] {
							break
						}
					}
					if i == len(hashlist) {
						diff = false
					}
				}
			}
			if diff {
				changed[f.Name()] = true
				if !ok {
					localMap[f.Name()] = &FileMetaData{}
					localMap[f.Name()].Filename = f.Name()
					localMap[f.Name()].Version = 1
				} else {
					localMap[f.Name()].Version++
				}
				localMap[f.Name()].BlockHashList = hashlist
			}
		}
	}
	// deleted files
	for fname, data := range localMap {
		_, ok := curFiles[fname]
		if !ok {
			if len(data.BlockHashList) != 1 || data.BlockHashList[0] != "0" {
				changed[fname] = true
				data.Version++
				data.BlockHashList = make([]string, 1)
				data.BlockHashList[0] = "0"
			}
		}
	}
	return changed
}

func readFile(FilePath string, size int) []string {
	var hashlist []string

	f, err := os.Open(FilePath)
	if err != nil {
		hashlist = append(hashlist, "0")
		return hashlist
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	buf := make([]byte, size)
	result := bytes.NewBuffer(nil)

	for {
		n, err := reader.Read(buf) // read up to len(buf) bytes, maybe lesser
		if err != nil && err != io.EOF {
			return nil
		}
		if err == io.EOF {
			break
		}

		result.Write(buf[0:n])
		if result.Len() > size {
			hash := GetBlockHashString(result.Next(size))
			hashlist = append(hashlist, hash)
		}
	}
	if result.Len() > 0 {
		lsize := result.Len()
		hash := GetBlockHashString(result.Next(lsize))
		hashlist = append(hashlist, hash)
	}
	if len(hashlist) == 0 {
		hashlist = append(hashlist, "-1")
	}

	return hashlist
}

func writeFile(outputfile string, blocks []*Block) {
	f, err := os.Create(outputfile)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	for n := 0; n < len(blocks); n++ {
		f.Write(blocks[n].BlockData)
		if err != nil {
			log.Fatal(err)
		}
	}
}
