package mapreduce

import (
	"encoding/json"
	"fmt"
	"os"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	//outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	outFile := mergeName(jobName, reduceTaskNumber)
	kvMap := make(map[string][]string)

	for i := 0; i < nMap; i++ {
		filename := reduceName(jobName, i, reduceTaskNumber)
		fin, err := os.Open(filename)
		checkError(err, fmt.Sprintf("Failed to open intermediate file: %s", filename))

		var kv KeyValue
		dec := json.NewDecoder(fin)

		for dec.More() {
			err := dec.Decode(&kv)
			checkError(err, fmt.Sprintf("Failed to decode file: %s", filename))
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}

		fin.Close()
	}

	// Write reduce output files
	fout, err := os.Create(outFile)
	defer fout.Close()
	checkError(err, fmt.Sprintf("Failed to create reduce output file: %s", outFile))
	enc := json.NewEncoder(fout)

	for k, values := range kvMap {
		v := reduceF(k, values)
		enc.Encode(&KeyValue{k, v})
	}
}
