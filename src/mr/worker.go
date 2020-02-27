package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []mr.KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func readFile(filename string) string {
    file, err := os.Open(filename)
    if err != nil {
        log.Fatalf("cannot open %v", filename)
    }
    content, err := ioutil.ReadAll(file)
    if err != nil {
        log.Fatalf("cannot read %v", filename)
    }
    file.Close()

    return string(content)

}

func  loadAllMapInterFile(filenames []string) []mr.KeyValue {
    kva := []mr.KeyValue{}
	for _, filename := range filenames {
		file, err := os.Open(filename)
        dec := json.NewDecoder(file)
        for {
            var kv KeyValue
            if err := dec.Decode(&kv); err != nil {
              break
            }
            kva = append(kva, kv)
        }
    }

    return kva
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

	// ask a job
	args := AskJobArgs{}
    args.workid = 1
	reply := AskJobReply{}
	call("Master.GetOneJob", &args, &reply)

    if reply.jobtype == "MAP" {
        nReduce := reply.nReduce
        content := readFile(reply.filename)
        kva := mapf(reply.filename, string(content))
        sort.Slice(kva, func(i, j int) bool { return ihash(kva[i].Key) % nReduce < ihash(kva[j].Key) % nReduce } )

        taskidStr := string(reply.taskid)
        preReduceId := -1
        var enc *Encoder = nil
        for _, kv := range kva {
            reduceid := ihash(kv.Key) % reply.nReduce
            if reduceid != preReduceId {
                preReduceId = reduceid
                filename = "mr-" + taskidStr + "-" + string(reduceid)
                file, err := os.Open(filename)
                if err != nil {
                    log.Fatalf("cannot open %v", filename)
                }
                enc := json.NewEncoder(file)
            }
            err := enc.Encode(&kv)
        }
    }else if reply.jobtype == "REDUCE" {

        kva := loadAllMapInterFile(reply.filenames)
        sort.Sort(ByKey(intermediate))

        oname := reply.outfile
        ofile, _ := os.Create(oname)

        i := 0
        for i < len(kva) {
            j := i + 1
            for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
                j++
            }
            values := []string{}
            for k := i; k < j; k++ {
                values = append(values, intermediate[k].Value)
            }
            output := reducef(intermediate[i].Key, values)
            fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
            i = j

        }
    }

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
