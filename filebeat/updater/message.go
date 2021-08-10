package updater

import "time"

type Progress struct {
	HostName  string    `json:"hostname"`
	TimeStamp int64     `json:"timestamp"`
	Clusters  []Cluster `json:"clusters"`
}

type Cluster struct {
	Name      string     `json:"name"`
	PathBases []PathBase `json:"path_bases"`
}

type PathBase struct {
	Base  string `json:"base"`
	Files []File `json:"files"`
}

type File struct {
	Path   string `json:"path"`
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	Offset int64  `json:"offset"`
}

type Data struct {
	Clusters []ClusterConfig `json:"clusters"`
}

type ClusterConfig struct {
	Name    string   `json:"cluster"`
	Hosts   []string `json:"address"`
	Configs []Config `json:"config"`
}

type Config struct {
	Enabled       string `json:"switch"`
	PathBase      string `json:"path"`
	StartPosition string `json:"start_position"`
	Topic         string `json:"topic_id"`
	Prefix        string `json:"msg_prefix"`
	CustomePrefix string `json:"custom_msg_prefix"`
}

type ConfigResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    Data   `json:"data"`
}

type Field struct {
	LogTopic  string `yaml:"log_topic"`
	MsgPrefix string `yaml:"msg_prefix"`
}

type Input struct {
	Type     string `yaml:"type"`
	Enabled  bool   `yaml:"enabled"`
	Symlinks bool   `yaml:"symlinks"`
	//Hosts         []string      `yaml:"hosts"`
	Paths         []string      `yaml:"paths"`
	TailFiles     bool          `yaml:"tail_files"`
	ScanFrequency time.Duration `yaml:"scan_frequency"`
	Backoff       time.Duration `yaml:"backoff"`
	MaxBackoff    time.Duration `yaml:"max_backoff"`
	Output        Output        `yaml:"output"`
}

type InputList struct {
	Inputs []Input `yaml:"filebeat.inputs"`
}

type Output struct {
	Hosts   string `yaml:"hosts"`
	Codec   string `yaml:"codec"`
	Prefix  string `yaml:"prefix"`
	Cluster string `yaml:"cluster"`
	Topic   string `yaml:"topic"`
}
