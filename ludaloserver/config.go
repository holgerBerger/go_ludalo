package main

type configT struct {
	Interval int                      // time between aggregation runs
	Systems  map[string]systemConfigT // Systems to be aggregated and served
}

type systemConfigT struct {
	Dbserver string // name:port
	Dbname   string // name of the DB, collection names are predefined
}
