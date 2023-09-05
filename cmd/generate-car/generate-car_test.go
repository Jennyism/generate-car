package main

import "testing"

func TestGetInputList(t *testing.T) {
	ret, err := getInputList("", false, "/private/var/folders/y0/l87thh5d72q8_9tfgw9vbr5h0000gn/T/com.jinghaoshe.ezip/1E8E12802E3E42D88641D8A840F4D15899679000001F53B04E8A8/ldn_2082_precar_1_20230803_1200.json")

	if err != nil {
		t.Fatal(err)
	}

	t.Log(ret)
}
