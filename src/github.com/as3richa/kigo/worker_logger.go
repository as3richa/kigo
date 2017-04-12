package kigo

import "github.com/sirupsen/logrus"

type nullWriter struct{}

func (nullWriter) Write(data []byte) (int, error) { return len(data), nil }

var NullWorkerLogger = logrus.New()
var DefaultWorkerLogger = logrus.New()

func init() {
	NullWorkerLogger.Out = nullWriter{}
}
