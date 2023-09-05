package util

import (
	"github.com/qiniupd/qiniu-go-sdk/syncdata/operation"
	"golang.org/x/xerrors"
)

func SubmitPath(configFilePath, key, path string) error {
	conf, err := operation.Load(configFilePath)
	if err != nil {
		return xerrors.Errorf("load QINIU config error")
	}

	if conf.Sim {
		return nil
	}

	uploader := operation.NewUploader(conf)
	return uploader.Upload(path, key)
}
