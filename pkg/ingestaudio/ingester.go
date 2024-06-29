package ingestaudio

import (
	"context"
	"emperror.dev/errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/je4/filesystem/v3/pkg/writefs"
	mediaserverproto "github.com/je4/mediaserverproto/v2/pkg/mediaserver/proto"
	"github.com/je4/utils/v2/pkg/checksum"
	"github.com/je4/utils/v2/pkg/zLogger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

func NewIngester(dbClient mediaserverproto.DatabaseClient, vfs fs.FS, concurrentWorkers int, ingestTimeout, ingestWait time.Duration, ffmpegPath, tempDir string, ffmpegOutputCodec map[string][]string, logger zLogger.ZLogger) (*IngesterAudio, error) {
	if concurrentWorkers < 1 {
		return nil, errors.New("concurrentWorkers must be at least 1")
	}
	if ingestTimeout < 1 {
		return nil, errors.New("ingestTimeout must not be 0")
	}
	i := &IngesterAudio{
		dbClient:          dbClient,
		ffmpegPath:        ffmpegPath,
		tempDir:           tempDir,
		ffmpegOutputCodec: ffmpegOutputCodec,
		end:               make(chan bool),
		jobChan:           make(chan *JobStruct),
		ingestTimeout:     ingestTimeout,
		ingestWait:        ingestWait,
		logger:            logger,
		vfs:               vfs,
	}
	i.jobChan, i.worker = NewWorkerPool(concurrentWorkers, ingestTimeout, i.doIngest, logger)

	return i, nil
}

type IngesterAudio struct {
	dbClient          mediaserverproto.DatabaseClient
	end               chan bool
	worker            io.Closer
	jobChan           chan *JobStruct
	ingestTimeout     time.Duration
	ingestWait        time.Duration
	logger            zLogger.ZLogger
	vfs               fs.FS
	ffmpegPath        string
	tempDir           string
	ffmpegOutputCodec map[string][]string
}
type WriterNopcloser struct {
	io.Writer
}

func (WriterNopcloser) Close() error { return nil }

func (i *IngesterAudio) doIngest(job *JobStruct) error {
	i.logger.Debug().Msgf("ingestvideo %s/%s", job.collection, job.signature)

	item, err := i.dbClient.GetItem(context.Background(), &mediaserverproto.ItemIdentifier{
		Collection: job.collection,
		Signature:  job.signature,
	})
	if err != nil {
		return errors.Wrapf(err, "cannot get item %s/%s", job.collection, job.signature)
	}

	var fullpath string
	if !strings.Contains(job.Path, "://") {
		fullpath = strings.Join([]string{job.Storage.Filebase, job.Path}, "/")
	} else {
		fullpath = job.Path
	}
	sourceReader, err := i.vfs.Open(fullpath)
	if err != nil {
		return errors.Wrapf(err, "cannot open %s", fullpath)
	}
	defer sourceReader.Close()

	folder := uuid.New().String()
	os.MkdirAll(filepath.Join(i.tempDir, folder), 0755)
	defer func() {
		os.RemoveAll(filepath.Join(i.tempDir, folder))
	}()
	var processes []*exec.Cmd
	var pipes []*io.PipeWriter
	if codec, ok := i.ffmpegOutputCodec["video"]; ok && slices.Contains(job.Missing, "$$video") {
		params := []string{"-i", "-"}
		params = append(params, codec...)
		params = append(params, filepath.ToSlash(filepath.Join(i.tempDir, folder, "video.mp4")))
		pr, pw := io.Pipe()
		process := exec.Command(i.ffmpegPath, params...)
		process.Stdin = pr
		process.Stdout = os.Stdout
		process.Stderr = os.Stderr
		processes = append(processes, process)
		pipes = append(pipes, pw)
	}
	if codec, ok := i.ffmpegOutputCodec["preview"]; ok && slices.Contains(job.Missing, "$$preview") {
		params := []string{"-i", "-"}
		params = append(params, codec...)
		params = append(params, filepath.ToSlash(filepath.Join(i.tempDir, folder, "preview.mp4")))
		pr, pw := io.Pipe()
		process := exec.Command(i.ffmpegPath, params...)
		process.Stdin = pr
		process.Stdout = os.Stdout
		process.Stderr = os.Stderr
		processes = append(processes, process)
		pipes = append(pipes, pw)
	}

	if codec, ok := i.ffmpegOutputCodec["wave"]; ok && slices.Contains(job.Missing, "$$wave") {
		params := []string{"-i", "-"}
		params = append(params, codec...)
		params = append(params, filepath.ToSlash(filepath.Join(i.tempDir, folder, "wave.png")))
		pr, pw := io.Pipe()
		process := exec.Command(i.ffmpegPath, params...)
		process.Stdin = pr
		process.Stdout = os.Stdout
		process.Stderr = os.Stderr
		processes = append(processes, process)
		pipes = append(pipes, pw)
	}

	if codec, ok := i.ffmpegOutputCodec["web"]; ok && slices.Contains(job.Missing, "$$web") {
		params := []string{"-i", "-"}
		params = append(params, codec...)
		params = append(params, filepath.ToSlash(filepath.Join(i.tempDir, folder, "web.m4a")))
		pr, pw := io.Pipe()
		process := exec.Command(i.ffmpegPath, params...)
		process.Stdin = pr
		process.Stdout = os.Stdout
		process.Stderr = os.Stderr
		processes = append(processes, process)
		pipes = append(pipes, pw)
	}
	/*
		if codec, ok := i.ffmpegOutputCodec["cover"]; ok && slices.Contains(job.Missing, "$$cover") {
			params0 = append(params0, codec...)
			params0 = append(params0, filepath.ToSlash(filepath.Join(i.tempDir, folder, "cover.png")))
		}
	*/
	for _, process := range processes {
		i.logger.Debug().Msgf("ffmpeg command: %s", process.String())
	}

	var writers = make([]io.Writer, len(pipes))
	for i, pw := range pipes {
		writers[i] = pw
	}
	multiWriter := io.MultiWriter(writers...)

	copyFuncResult := make(chan error)
	go func() {
		_, err := io.Copy(multiWriter, sourceReader)
		if err != nil {
			copyFuncResult <- errors.Wrap(err, "cannot copy source to multiwriter")
			return
		}
		for _, pw := range pipes {
			pw.Close()
		}
		copyFuncResult <- nil
	}()

	ffmpegResult := make(chan error)
	for _, subProcess := range processes {
		go func(subProcess *exec.Cmd) {
			n := &checksum.NullWriter{}
			defer io.Copy(n, subProcess.Stdin)
			if err := subProcess.Run(); err != nil {
				ffmpegResult <- errors.Wrap(err, "cannot run ffmpeg0")
				return
			}
			ffmpegResult <- nil
		}(subProcess)
	}

	var errs []error
	for i := 0; i < len(processes)+1; i++ {
		select {
		case err := <-copyFuncResult:
			if err != nil {
				errs = append(errs, err)
			}
		case err := <-ffmpegResult:
			if err != nil {
				errs = append(errs, err)
			}
		}
	}
	if len(errs) > 0 {
		return errors.Combine(errs...)
	}

	var public = item.GetPublic() || slices.Contains(item.GetPublicActions(), "audioviewer")
	var ingestType = mediaserverproto.IngestType_KEEP

	if _, ok := i.ffmpegOutputCodec["web"]; ok && slices.Contains(job.Missing, "$$web") {
		source := filepath.Join(i.tempDir, folder, "web.m4a")
		itemName := createCacheName(job.collection, job.signature+"$$web", source)
		itemPath := job.Storage.Filebase + "/" + filepath.ToSlash(filepath.Join(job.Storage.Subitemdir, itemName))
		if _, err := writefs.Copy(i.vfs, source, itemPath); err != nil {
			return errors.Wrapf(err, "cannot copy %s to %s", source, itemPath)
		}
		resp, err := i.dbClient.CreateItem(context.Background(), &mediaserverproto.NewItem{
			Identifier: &mediaserverproto.ItemIdentifier{
				Collection: job.collection,
				Signature:  job.signature + "$$web",
			},
			Parent: &mediaserverproto.ItemIdentifier{
				Collection: job.collection,
				Signature:  job.signature,
			},
			Urn:        itemPath,
			IngestType: &ingestType,
			Public:     &public,
		})
		if err != nil {
			return errors.Wrapf(err, "cannot create item %s/%s", job.collection, job.signature+"$$web")
		}
		i.logger.Info().Msgf("created item %s/%s: %s", job.collection, job.signature+"$$web", resp.GetMessage())
	}
	if _, ok := i.ffmpegOutputCodec["video"]; ok && slices.Contains(job.Missing, "$$video") {
		source := filepath.Join(i.tempDir, folder, "video.mp4")
		itemName := createCacheName(job.collection, job.signature+"$$video", source)
		itemPath := job.Storage.Filebase + "/" + filepath.ToSlash(filepath.Join(job.Storage.Subitemdir, itemName))
		if _, err := writefs.Copy(i.vfs, source, itemPath); err != nil {
			return errors.Wrapf(err, "cannot copy %s to %s", source, itemPath)
		}
		resp, err := i.dbClient.CreateItem(context.Background(), &mediaserverproto.NewItem{
			Identifier: &mediaserverproto.ItemIdentifier{
				Collection: job.collection,
				Signature:  job.signature + "$$video",
			},
			Parent: &mediaserverproto.ItemIdentifier{
				Collection: job.collection,
				Signature:  job.signature,
			},
			Urn:        itemPath,
			IngestType: &ingestType,
			Public:     &public,
		})
		if err != nil {
			return errors.Wrapf(err, "cannot create item %s/%s", job.collection, job.signature+"$$video")
		}
		i.logger.Info().Msgf("created item %s/%s: %s", job.collection, job.signature+"$$video", resp.GetMessage())
	}
	if _, ok := i.ffmpegOutputCodec["wave"]; ok && slices.Contains(job.Missing, "$$wave") {
		wave := filepath.Join(i.tempDir, folder, "wave.png")
		if err != nil {
			return errors.Wrapf(err, "cannot convert wave %s", wave)
		}
		waveSignature := fmt.Sprintf("%s$$wave", job.signature)
		itemName := createCacheName(job.collection, waveSignature, wave)
		targetPath := job.Storage.Filebase + "/" + filepath.ToSlash(filepath.Join(job.Storage.Subitemdir, itemName))
		if _, err := writefs.Copy(i.vfs, wave, targetPath); err != nil {
			return errors.Wrapf(err, "cannot copy '%s' to '%s'", wave, targetPath)
		}
		resp, err := i.dbClient.CreateItem(context.Background(), &mediaserverproto.NewItem{
			Identifier: &mediaserverproto.ItemIdentifier{
				Collection: job.collection,
				Signature:  waveSignature,
			},
			Parent: &mediaserverproto.ItemIdentifier{
				Collection: job.collection,
				Signature:  job.signature,
			},
			Urn:        targetPath,
			IngestType: &ingestType,
			Public:     &public,
		})
		if err != nil {
			return errors.Wrapf(err, "cannot create item %s/%s", job.collection, waveSignature)
		}
		i.logger.Info().Msgf("created item %s/%s: %s", job.collection, waveSignature, resp.GetMessage())
	}
	if _, ok := i.ffmpegOutputCodec["preview"]; ok && slices.Contains(job.Missing, "$$preview") {
		source := filepath.Join(i.tempDir, folder, "preview.mp4")
		targetSignature := job.signature + "$$preview"
		itemName := createCacheName(job.collection, targetSignature, source)
		itemPath := job.Storage.Filebase + "/" + filepath.ToSlash(filepath.Join(job.Storage.Subitemdir, itemName))
		if _, err := writefs.Copy(i.vfs, source, itemPath); err != nil {
			return errors.Wrapf(err, "cannot copy %s to %s", source, itemPath)
		}
		resp, err := i.dbClient.CreateItem(context.Background(), &mediaserverproto.NewItem{
			Identifier: &mediaserverproto.ItemIdentifier{
				Collection: job.collection,
				Signature:  targetSignature,
			},
			Parent: &mediaserverproto.ItemIdentifier{
				Collection: job.collection,
				Signature:  job.signature,
			},
			Urn:        itemPath,
			IngestType: &ingestType,
			Public:     &public,
		})
		if err != nil {
			return errors.Wrapf(err, "cannot create item %s/%s", job.collection, targetSignature)
		}
		i.logger.Info().Msgf("created item %s/%s: %s", job.collection, targetSignature, resp.GetMessage())
	}
	return nil
}

func (i *IngesterAudio) Start() error {
	go func() {
		for {
			for {
				item, err := i.dbClient.GetDerivateIngestItem(context.Background(), &mediaserverproto.DerivatIngestRequest{
					Type:    "audio",
					Subtype: "",
					Suffix:  []string{"$$web", "$$video", "$$preview", "$$wave"},
				})
				if err != nil {
					if s, ok := status.FromError(err); ok {
						if s.Code() == codes.NotFound {
							i.logger.Info().Msg("no ingest item available")
						} else {
							i.logger.Error().Err(err).Msg("cannot get ingest item")
						}
					} else {
						i.logger.Error().Err(err).Msg("cannot get ingest item")
					}
					break // on all errors we break
				}
				cache, err := i.dbClient.GetCache(context.Background(), &mediaserverproto.CacheRequest{
					Identifier: item.Item.GetIdentifier(),
					Action:     "item",
					Params:     "",
				})
				if err != nil {
					i.logger.Error().Err(err).Msgf("cannot get cache %s/%s/item", item.Item.GetIdentifier().GetCollection(), item.Item.GetIdentifier().GetSignature())
					break
				}
				job := &JobStruct{
					collection: item.Item.GetIdentifier().GetCollection(),
					signature:  item.Item.GetIdentifier().GetSignature(),
					Width:      cache.GetMetadata().GetWidth(),
					Height:     cache.GetMetadata().GetHeight(),
					Duration:   cache.GetMetadata().GetDuration(),
					Size:       cache.GetMetadata().GetSize(),
					MimeType:   cache.GetMetadata().GetMimeType(),
					Path:       cache.GetMetadata().GetPath(),
					Missing:    item.GetMissing(),
					Storage: &storageStruct{
						Name:       cache.GetMetadata().GetStorage().GetName(),
						Filebase:   cache.GetMetadata().GetStorage().GetFilebase(),
						Datadir:    cache.GetMetadata().GetStorage().GetDatadir(),
						Subitemdir: cache.GetMetadata().GetStorage().GetSubitemdir(),
						Tempdir:    cache.GetMetadata().GetStorage().GetTempdir(),
					},
				}
				i.jobChan <- job
				i.logger.Debug().Msgf("ingest video item %s/%s", job.collection, job.signature)
				// check for end without blocking
				select {
				case <-i.end:
					close(i.end)
					return
				default:
				}
			}
			select {
			case <-i.end:
				close(i.end)
				return
			case <-time.After(i.ingestWait):
			}
		}
	}()
	return nil
}

func (i *IngesterAudio) Close() error {
	i.end <- true
	return i.worker.Close()
}
