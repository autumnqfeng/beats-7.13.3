package updater

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/elastic/beats/v7/filebeat/registrar"
	"github.com/elastic/beats/v7/libbeat/common"
	helper "github.com/elastic/beats/v7/libbeat/common/file"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

const (
	debugSelector      = "updater"
	fetchConfigPath    = "/qbus/logcollect-service/config"
	reportProgressPath = "/qbus/logcollect-service/progress"
	defaultConfigPath  = "/etc/filebeat/input.d/input.yml"
)

var debugf = logp.MakeDebug(debugSelector)

type Updater struct {
	cfg *common.Config
	//client         http.Client
	clustersConfig map[string]InputList
	Period         time.Duration
	done           chan struct{}
	puller         *Puller
	reporter       *Reporter
}

type Client struct {
	Host       string
	user       string
	password   string
	httpClient http.Client
}

type Puller struct {
	client         *Client
	hostname       string
	configPath     string
	clustersConfig map[string]InputList
}

type Reporter struct {
	hostname  string
	client    *Client
	registrar *registrar.Registrar
}

func New(config *common.Config, registrar *registrar.Registrar) (*Updater, error) {
	return &Updater{
		cfg:  config,
		done: make(chan struct{}, 1),
		reporter: &Reporter{
			registrar: registrar,
		},
	}, nil
}

func (u *Updater) Start() error {
	config := defaultConfig
	if err := u.cfg.Unpack(&config); err != nil {
		return errors.Wrap(err, "reading updater reporter config error")
	}
	logp.Info("updater starting with config: %v", config)
	if config.Enabled == false {
		logp.Info("updater report is disable.")
		return nil
	}

	hostname, err := os.Hostname()
	if err != nil {
		logp.Warn("updater report can't get hostname")
		return err
	}
	client := Client{
		Host:     config.Host,
		user:     config.User,
		password: config.Password,
		httpClient: http.Client{
			Timeout: config.Timeout * time.Second,
		},
	}
	puller := &Puller{
		client:         &client,
		hostname:       hostname,
		configPath:     config.Path,
		clustersConfig: make(map[string]InputList),
	}
	u.puller = puller
	u.reporter.client = &client
	u.reporter.hostname = hostname
	u.Period = config.Period

	if err := u.Init(config.Path); err != nil {
		logp.Warn("updater can't init: %v", err)
		return err
	}

	go u.Run()
	logp.Info("Starting updater report service success")
	return nil
}

func (u *Updater) Run() {
	timer := time.NewTicker(u.Period)
	//timer := time.NewTicker(time.Second * r.work.Period)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			config, err := u.puller.pullConfig()
			if err != nil {
				logp.Info("updater pull input config error: %s", err)
				continue
			}
			if err := u.reporter.reportProgress(config); err != nil {
				logp.Info("updater reporter error: %s", err)
			}
		case <-u.done:
			logp.Info("updater Stoped")
			return
		}
	}
}

func (u *Updater) Stop() {
	u.done <- struct{}{}
}

func (c *Client) newRequest(url string, body interface{}, params map[string]string, method string) (*http.Request, error) {
	var reader io.Reader
	var req *http.Request
	if body != nil {
		switch body.(type) {
		case []byte:
			reader = bytes.NewReader(body.([]byte))
		default:
			bodyJson, err := json.Marshal(body)
			if err != nil {
				return nil, err
			}
			reader = bytes.NewBuffer(bodyJson)
		}
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		return nil, errors.New("new request is fail: %v \n")
	}
	q := req.URL.Query()
	if params != nil {
		for key, val := range params {
			q.Add(key, val)
		}
		req.URL.RawQuery = q.Encode()
	}
	req.SetBasicAuth(c.user, c.password)
	return req, nil
}

func (c *Client) doRequest(request *http.Request) ([]byte, error) {
	resp, err := c.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, errors.Errorf("updater http response StatusCode: %d", resp.StatusCode)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "updater read http response body error")
	}
	return body, nil
}

func parseMessage(body []byte, result interface{}) error {
	return json.Unmarshal(body, result)
}

func (r *Reporter) getFileState() map[string]File {
	fileStats := make(map[string]File)
	states := r.registrar.GetStates()

	for _, state := range states {
		// Fetch Stat file info which fetches the inode. In case of a symlink, the original inode is fetched
		// TODO: check symlink if config symlink is disable
		fileInfo, err := os.Stat(state.Source)
		if err != nil {
			debugf("stat(%s) failed: %s", state.Source, err)
			continue
		}
		file := File{
			Path:   state.Source,
			Name:   fileInfo.Name(),
			Size:   fileInfo.Size(),
			Offset: state.Offset,
		}
		fileStats[state.Source] = file
	}
	debugf("get all the file state: %v", fileStats)
	return fileStats
}

func (r *Reporter) NewProgressMessage(stats map[string]File, clustersConfig map[string]InputList) (Progress, error) {
	progress := Progress{
		HostName:  r.hostname,
		TimeStamp: time.Now().Unix(),
		Clusters:  make([]Cluster, 0),
	}
	for clusterName, input := range clustersConfig {
		cluster := Cluster{
			Name:      clusterName,
			PathBases: make([]PathBase, 0),
		}
		for _, input := range input.Inputs {
			for _, path := range input.Paths {
				pathBase := PathBase{
					Base:  path,
					Files: make([]File, 0),
				}
				filesPaths := getFiles(path)
				for _, filePath := range filesPaths {
					fileInfo, ok := stats[filePath]
					if ok == false {
						//TODO or just create a file with zero offset?
						continue
					}
					pathBase.Files = append(pathBase.Files, fileInfo)
				}
				cluster.PathBases = append(cluster.PathBases, pathBase)
			}
		}
		progress.Clusters = append(progress.Clusters, cluster)
	}
	debugf("progress message: %v", progress)
	return progress, nil
}

func getFiles(path string) []string {
	files := make([]string, 0)
	matches, err := filepath.Glob(path)
	if err != nil {
		logp.Err("glob(%s) failed: %v", path, err)
		return files
	}
	// Check any matched files
	for _, file := range matches {

		//TODO check if the file is in the exclude_files list
		//TODO check if the file is symlink

		// Fetch Lstat File info to detected also symlinks
		fileInfo, err := os.Lstat(file)
		if err != nil {
			debugf("lstat(%s) failed: %s", file, err)
			continue
		}

		if fileInfo.IsDir() {
			debugf("Skipping directory: %s", file)
			continue
		}

		isSymlink := fileInfo.Mode()&os.ModeSymlink > 0
		if isSymlink {
			debugf("File %s it is a symlink.", file)
		}
		files = append(files, file)
	}
	return files
}

func (r *Reporter) doHttpRequest(body Progress) error {
	url := "http://" + r.client.Host + reportProgressPath
	request, err := r.client.newRequest(url, body, nil, http.MethodPut)
	if err != nil {
		return errors.Wrap(err, "reporter new http request error")
	}
	_, err = r.client.doRequest(request)
	if err != nil {
		return errors.Wrap(err, "reporter do http request error")
	}
	return nil
}

func (r *Reporter) reportProgress(config map[string]InputList) error {
	message, err := r.NewProgressMessage(r.getFileState(), config)
	if err != nil {
		return err
	}
	err = r.doHttpRequest(message)
	if err != nil {
		return err
	}
	return nil
}

func (p *Puller) getConfigPath() string {
	return p.configPath
}

func (p *Puller) fetchConfig() (ConfigResponse, error) {
	config := ConfigResponse{}
	message, err := p.doHttpRequest()
	if err != nil {
		return config, errors.Wrap(err, "puller new http request error")
	}
	err = parseMessage(message, &config)
	if err != nil {
		return config, errors.Wrap(err, fmt.Sprintf("puller parser http response error, http body:%s", string(message)))
	}
	return config, nil
}

func (p *Puller) doHttpRequest() ([]byte, error) {
	url := "http://" + p.client.Host + fetchConfigPath
	query := map[string]string{"hostname": p.hostname}
	request, err := p.client.newRequest(url, nil, query, http.MethodGet)
	if err != nil {
		return nil, errors.Wrap(err, "puller new http request error")
	}
	body, err := p.client.doRequest(request)
	if err != nil {
		return nil, errors.Wrap(err, "puller do http request error")
	}
	return body, nil
}

func (p *Puller) pullConfig() (map[string]InputList, error) {
	config, err := p.fetchConfig()
	if err != nil {
		return nil, errors.Wrap(err, "puller fetch config error")
	}
	clusterConfig := p.newClusterConfig(&config)
	if configIsChanged(clusterConfig, p.clustersConfig) == false {
		return clusterConfig, nil
	}
	p.clustersConfig = clusterConfig
	configString, err := convertToString(clusterConfig)
	if err != nil {
		return nil, errors.Wrap(err, "puller convert config to string error")
	}
	err = persistToFile(configString, p.configPath)
	if err != nil {
		return nil, errors.Wrap(err, "puller persist config to file error")
	}
	return clusterConfig, nil
}

func (p *Puller) newClusterConfig(config *ConfigResponse) map[string]InputList {
	clustersConfig := make(map[string]InputList)
	for _, cluster := range config.Data.Clusters {
		inputs := make([]Input, 0)
		for _, item := range cluster.Configs {
			input := NewInput()
			if item.Enabled == "on" {
				input.Enabled = true
			}
			input.Output.Hosts = strings.Join(cluster.Hosts, ",")
			input.Output.Cluster = cluster.Name
			input.Output.Topic = item.Topic
			input.Paths = append(input.Paths, item.PathBase)
			input.Output.Prefix = p.getPrefix(item.Prefix, item.PathBase)
			inputs = append(inputs, input)
		}
		if inputList, ok := clustersConfig[cluster.Name]; ok == true {
			inputList.Inputs = append(inputList.Inputs, inputs...)
		} else {
			InputList := InputList{
				Inputs: inputs,
			}
			clustersConfig[cluster.Name] = InputList
		}
	}
	return clustersConfig
}

func NewInput() Input {
	input := Input{
		Type:          "log",
		Enabled:       false,
		Symlinks:      true,
		Paths:         make([]string, 0),
		ScanFrequency: 10 * time.Second,
		MaxBackoff:    10 * time.Second,
		Backoff:       1 * time.Second,
		TailFiles:     true,
		Output: Output{
			Codec: "format",
		},
	}
	return input
}

func (p *Puller) getPrefix(option string, path string) string {
	var result string
	switch option {
	case "hostname":
		result = "[" + p.hostname + "]"
	case "path":
		result = "[" + path + "]"
	case "both":
		result = "[" + p.hostname + path + "]"
	default:
	}
	return result
}

func persistToFile(config, path string) error {
	tempFile, err := writeTmpFile(path, 0666, config)
	if err != nil {
		return err
	}
	err = helper.SafeFileRotate(path, tempFile)
	if err != nil {
		return err
	}

	logp.Info("Write input config to file: %s success", path)
	return nil
}

func convertToString(config map[string]InputList) (string, error) {
	var buffer bytes.Buffer
	for _, e := range config {
		tmp, err := yaml.Marshal(e)
		if err != nil {
			return "", err
		}
		if _, err := buffer.Write(tmp); err != nil {
			return "", err
		}
	}
	return strings.Replace(buffer.String(), "filebeat.inputs:\n", "", -1), nil
}

func configIsChanged(newConfig, originConfig map[string]InputList) bool {
	return !reflect.DeepEqual(originConfig, newConfig)
}

func (u *Updater) Init(file string) error {
	// Create directory if it does not already exist.
	configPath := filepath.Dir(file)
	err := os.MkdirAll(configPath, 0750)
	if err != nil {
		return fmt.Errorf("Failed to created input config file dir %s: %v", configPath, err)
	}

	// Check if files exists
	fileInfo, err := os.Lstat(file)
	if os.IsNotExist(err) {
		logp.Info("No input config file found under: %s. Creating a new input config file.", file)
		// No input config file exists yet, write empty string to check if input config file can be written
		return persistToFile("", u.puller.getConfigPath())
	}
	if err != nil {
		return err
	}

	// Check if regular file, no dir, no symlink
	if !fileInfo.Mode().IsRegular() {
		// Special error message for directory
		if fileInfo.IsDir() {
			return fmt.Errorf("updater input config file path must be a file. %s is a directory.", file)
		}
		return fmt.Errorf("updater input config file path is not a regular file: %s", file)
	}

	debugf("updater input config file set to: %s", file)
	return nil
}

func writeTmpFile(baseName string, perm os.FileMode, content string) (string, error) {
	debugf("Write input config file: %s (%v)", baseName, len(content))

	tempFile := baseName + ".new"
	f, err := os.OpenFile(tempFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_SYNC, perm)
	if err != nil {
		logp.Err("Failed to create tempFile (%s) for writing: %s", tempFile, err)
		return "", err
	}

	defer f.Close()

	_, err = io.WriteString(f, content)
	if err != nil {
		return "", err
	}

	// Commit the changes to storage to avoid corrupt input config files
	if err = f.Sync(); err != nil {
		logp.Err("Error when syncing new input config file contents: %s", err)
		return "", err
	}

	return tempFile, nil
}
