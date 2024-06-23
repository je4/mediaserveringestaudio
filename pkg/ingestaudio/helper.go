package ingestaudio

import (
	"github.com/je4/mediaserveraction/v2/pkg/actionController"
	"path/filepath"
)

func createCacheName(collection, signature, source string) string {
	ext := filepath.Ext(source)
	name := actionController.CreateCacheName(collection, signature, "item", "", ext)
	return name
}
