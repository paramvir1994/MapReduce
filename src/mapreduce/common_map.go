package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
	c, err := ioutil.ReadFile(inFile)
	checkError(err, fmt.Sprintf("Failed to read file: %s for map task", inFile))

	mapResults := mapF(inFile, string(c))

	// Creating the intermediate files
	encoders := make([]*json.Encoder, nReduce)

	for i := 0; i < nReduce; i++ {
		fname := reduceName(jobName, mapTaskNumber, i)
		f, err := os.Create(fname)
		defer f.Close()
		checkError(err, fmt.Sprintf("Failed to create intermediate output file: %s", fname))

		encoders[i] = json.NewEncoder(f)
	}

	// Writing the map results to intermediate file in json format
	for _, kv := range mapResults {
		index := ihash(kv.Key) % uint32(nReduce)
		err := encoders[index].Encode(&kv)
		checkError(err, fmt.Sprintf("Failed to encode: %s to file", kv))
	}
}

func checkError(err error, logging string) {
	if err != nil {
		log.Fatalf("%s, %s", logging, err)
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
