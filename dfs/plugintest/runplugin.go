package main

import "plugin"

//go build -buildmode=plugin -gcflags="all=-N -l" -o dfs/plugintest/plugin.so dfs/plugin/plugin.go

func main() {

	p, err := plugin.Open("dfs/plugintest/plugin.so")
	if err != nil {
		panic(err)
	}
	v, err := p.Lookup("V")
	if err != nil {
		panic(err)
	}
	f, err := p.Lookup("F")
	if err != nil {
		panic(err)
	}
	*v.(*int) = 7
	f.(func())() // prints "Hello, number 7"
}
