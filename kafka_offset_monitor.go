package main

import (
	"flag"
	"fmt"
	zk "github.com/samuel/go-zookeeper/zk"
	"os"
	"strings"
	"time"
	"strconv"
)

var (
	zklist     string
	consumer   string
	topic      string
	old_offset int
	new_offset int
	lag        int
)

func main() {
	flag.StringVar(&zklist, "zklist", "localhost:2181", "zookeeperlist")
	flag.StringVar(&consumer, "consumer", "comsumer-group-test", "consumer")
	flag.StringVar(&topic, "topic", "topic", "topic")

	flag.Parse()

	old_offset = 0
	new_offset = 0

	zkconn, _, err := zk.Connect(strings.Split(zklist, ","), 5*time.Second)

	defer zkconn.Close()
	if err != nil {
		panic(err)
		os.Exit(1)
	}

	topic_path := "/consumers/" + consumer + "/" + "offsets" + "/" +topic

	partition, _, err := zkconn.Children(topic_path)

	if err != nil {
		panic(err)
		os.Exit(1)
	}

	for _,offset_d := range partition {
		rs_byte,_,_ := zkconn.Get(topic_path + "/" + offset_d)
		rs_int,_ := strconv.Atoi(string(rs_byte[:]))
		old_offset += rs_int
	}
	time.Sleep(1)
	for _,offset_d := range partition {
		rs_byte,_,_ := zkconn.Get(topic_path + "/" + offset_d)
		rs_int,_ := strconv.Atoi(string(rs_byte[:]))
		new_offset += rs_int
	}

	lag = new_offset - old_offset
	fmt.Println(topic + ":",lag)


}
