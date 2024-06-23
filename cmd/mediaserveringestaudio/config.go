package main

import (
	"io/fs"
	"os"

	"emperror.dev/errors"
	"github.com/BurntSushi/toml"
	"github.com/je4/filesystem/v3/pkg/vfsrw"
	loaderConfig "github.com/je4/trustutil/v2/pkg/config"
	configutil "github.com/je4/utils/v2/pkg/config"
	"github.com/je4/utils/v2/pkg/zLogger"
)

type MediaserverIngestAudioConfig struct {
	LocalAddr               string                  `toml:"localaddr"`
	ResolverAddr            string                  `toml:"resolveraddr"`
	ResolverTimeout         configutil.Duration     `toml:"resolvertimeout"`
	ResolverNotFoundTimeout configutil.Duration     `toml:"resolvernotfoundtimeout"`
	FfmpegPath              string                  `toml:"ffmpegpath"`
	TempDir                 string                  `toml:"tempdir"`
	FfmpegOutputCodec       map[string][]string     `toml:"ffmpegoutputcodec"`
	IngestTimeout           configutil.Duration     `toml:"ingesttimeout"`
	IngestWait              configutil.Duration     `toml:"ingestwait"`
	ConcurrentTasks         int                     `toml:"concurrenttasks"`
	GRPCClient              map[string]string       `toml:"grpcclient"`
	ClientTLS               *loaderConfig.TLSConfig `toml:"client"`

	VFS map[string]*vfsrw.VFS `toml:"vfs"`
	Log zLogger.Config        `toml:"log"`
}

func LoadMediaserverIngestAudioConfig(fSys fs.FS, fp string, conf *MediaserverIngestAudioConfig) error {
	if _, err := fs.Stat(fSys, fp); err != nil {
		path, err := os.Getwd()
		if err != nil {
			return errors.Wrap(err, "cannot get current working directory")
		}
		fSys = os.DirFS(path)
		fp = "mediaserveringestaudio.toml"
	}
	data, err := fs.ReadFile(fSys, fp)
	if err != nil {
		return errors.Wrapf(err, "cannot read file [%v] %s", fSys, fp)
	}
	_, err = toml.Decode(string(data), conf)
	if err != nil {
		return errors.Wrapf(err, "error loading config file %v", fp)
	}
	return nil

}
