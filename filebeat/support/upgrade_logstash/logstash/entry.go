package logstash

type SinceDB struct {
	Path   string
	Inode  uint64
	Offset int64
}

type Config struct {
	Cluster        string
	Path           string
	Codec          string
	Prefix         string
	Topic          string
	OutputAddress  string
	SinceDbPath    string
	InodeOffsetMap map[uint64]int64
}

func newConfig() *Config {
	return &Config{}
}

func (c *Config) copy() *Config {
	return &Config{
		Cluster:       c.Cluster,
		Path:          c.Path,
		Codec:         c.Codec,
		Prefix:        c.Prefix,
		Topic:         c.Topic,
		OutputAddress: c.OutputAddress,
		SinceDbPath:   c.SinceDbPath,
	}
}
