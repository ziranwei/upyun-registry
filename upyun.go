package upyun

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"path"
	"strings"
	"time"

	"github.com/docker/distribution/context"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
	"github.com/upyun/go-sdk/upyun"
)

const driverName = "upyun"

type DriverParameters struct {
	Username      string
	Password      string
	Bucket        string
	Endpoint      string
	RootDirectory string
}

func init() {
	factory.Register(driverName, &upyunDriverFactory{})
}

type upyunDriverFactory struct{}

func (factory *upyunDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

type driver struct {
	Client        *upyun.UpYun
	rootDirectory string
}

type baseEmbed struct {
	base.Base
}

type Driver struct {
	baseEmbed
}

func FromParameters(parameters map[string]interface{}) (*Driver, error) {
	username, ok := parameters["username"]
	if !ok {
		return nil, fmt.Errorf("No username parameter provided")
	}
	password, ok := parameters["password"]
	if !ok {
		return nil, fmt.Errorf("No password parameter provided")
	}

	bucket, ok := parameters["bucket"]
	if !ok || fmt.Sprint(bucket) == "" {
		return nil, fmt.Errorf("No bucket parameter provided")
	}

	endpoint, ok := parameters["endpoint"]
	if !ok {
		endpoint = "v0.api.upyun.com"
	}

	rootDirectory, ok := parameters["rootdirectory"]
	if !ok {
		rootDirectory = ""
	}

	params := DriverParameters{
		Username:      fmt.Sprint(username),
		Password:      fmt.Sprint(password),
		Bucket:        fmt.Sprint(bucket),
		Endpoint:      fmt.Sprint(endpoint),
		RootDirectory: fmt.Sprint(rootDirectory),
	}

	return New(params)
}

func New(params DriverParameters) (*Driver, error) {
	client := upyun.NewUpYun(params.Bucket, params.Username, params.Password)
	if client == nil {
		return nil, nil
	}

	d := &driver{
		Client:        client,
		rootDirectory: params.RootDirectory,
	}

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: d,
			},
		},
	}, nil
}

func (d *driver) Name() string {
	return driverName
}

func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	fullPath := d.fullPath(path)

	if _, err := d.Client.GetInfo(fullPath); err != nil {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}

	b := bytes.NewBuffer(make([]byte, 0))
	if _, err := d.Client.Get(fullPath, b); err != nil {
		return nil, err
	}

	buf, err := ioutil.ReadAll(b)

	return buf, err
	rc, err := d.Reader(ctx, path, 0)
	if err != nil {
		return nil, err
	}

	p, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {
	fullPath := d.fullPath(path)
	if err := d.Client.Mkdir(fullPath[:strings.LastIndex(fullPath, "/")]); err != nil {
		return storagedriver.InvalidPathError{Path: path}
	}
	for {
		body := bytes.NewBuffer(contents)
		_, err := d.Client.Put(fullPath, body, false, nil)
		if err != nil {
			return storagedriver.InvalidPathError{Path: path}
		}

		fi, err := d.Client.GetInfo(fullPath)
		if err != nil {
			return storagedriver.InvalidPathError{Path: path}
		}

		if fi.Size == int64(len(contents)) {
			break
		}
		time.Sleep(time.Second)
	}
	return nil
}

func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	fullPath := d.fullPath(path)
	w := bytes.NewBuffer(make([]byte, 0))
	length, err := d.Client.Get(fullPath, w)
	if err != nil {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}
	if offset > int64(length) {
		return nil, storagedriver.InvalidOffsetError{Path: path, Offset: offset}
	}

	return ioutil.NopCloser(bytes.NewReader(w.Bytes()[offset:])), nil
}

func (d *driver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	fullPath := d.fullPath(path)
	var offset int64
	if append {
		fi, err := d.Client.GetInfo(fullPath)
		if err != nil {
			return nil, storagedriver.PathNotFoundError{Path: path}
		}

		offset = fi.Size
	}
	return d.newWriter(path, offset), nil
}

func (d *driver) Delete(ctx context.Context, sourcePath string) error {
	fi, err := d.Stat(ctx, sourcePath)
	if err != nil {
		return err
	}
	if fi.IsDir() {
		files, err := d.List(ctx, sourcePath)
		if err != nil {
			return err
		}

		for _, file := range files {
			d.Delete(ctx, file)
		}
		for {
			if err := d.Client.Delete(d.fullPath(sourcePath)); err == nil {
				break
			}
			time.Sleep(time.Second)
		}
	} else {
		for {
			if err := d.Client.Delete(d.fullPath(sourcePath)); err == nil {
				break
			}
			time.Sleep(time.Second)
		}
	}

	return nil
}

func (d *driver) List(ctx context.Context, path string) ([]string, error) {
	fullPath := d.fullPath(path)
	files := []string{}
	fileChan, errChan := d.Client.GetLargeList(fullPath, false, false)

	for e := range errChan {
		fmt.Println(e)
		return nil, storagedriver.PathNotFoundError{Path: path}
	}

	for f := range fileChan {
		files = append(files, path+"/"+f.Name)
	}

	return files, nil
}

func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	fi, err := d.Stat(ctx, sourcePath)
	if err != nil {
		return err
	}
	if fi.IsDir() {
		files, err := d.List(ctx, sourcePath)
		if err != nil {
			return err
		}

		for _, file := range files {
			d.Move(ctx, sourcePath+"/"+file, destPath+"/"+file)
		}
	} else {
		b, err := d.GetContent(ctx, sourcePath)
		if err != nil {
			return err
		}

		for {
			if err := d.PutContent(ctx, destPath, b); err != nil {
				return nil
			}
			fullPath := d.fullPath(destPath)
			fi, err := d.Client.GetInfo(fullPath)
			if err != nil {
				return err
			}

			if fi.Size == int64(len(b)) {
				break
			}
			time.Sleep(time.Second)
		}

		d.Client.AsyncDelete(d.fullPath(sourcePath))
	}

	return nil
}

func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	fullPath := d.fullPath(path)
	fi, err := d.Client.GetInfo(fullPath)
	if err != nil {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}

	return storagedriver.FileInfoInternal{
		FileInfoFields: storagedriver.FileInfoFields{
			Size:  fi.Size,
			Path:  fi.Name,
			IsDir: fi.Type != "file",
		},
	}, nil
}

func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	return "", storagedriver.ErrUnsupportedMethod{}
}

func (d *driver) fullPath(subPath string) string {
	return path.Join(d.rootDirectory, subPath)
}

type writer struct {
	driver    *driver
	key       string
	size      int64
	readyPart []byte
	closed    bool
	committed bool
	cancelled bool
}

func (d *driver) newWriter(key string, size int64) storagedriver.FileWriter {
	w := bytes.NewBuffer(make([]byte, 0))
	if size > 0 {
		fullPath := d.fullPath(key)
		_, err := d.Client.Get(fullPath, w)
		if err != nil {
			body := bytes.NewBuffer([]byte(""))
			d.Client.Put(fullPath, body, false, nil)
		}
	}

	return &writer{
		driver:    d,
		key:       key,
		size:      size,
		readyPart: w.Bytes(),
		closed:    false,
		committed: false,
		cancelled: false,
	}
}

func (w *writer) Cancel() error {
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	}
	w.size = 0
	w.cancelled = true

	return nil
}

func (w *writer) Close() error {
	if w.closed {
		return fmt.Errorf("already closed")
	}

	fullPath := w.driver.fullPath(w.key)

	for {
		body := bytes.NewBuffer(w.readyPart)
		if err := w.driver.Client.Mkdir(fullPath[:strings.LastIndex(fullPath, "/")]); err != nil {
			return storagedriver.InvalidPathError{Path: w.key}
		}
		if _, err := w.driver.Client.Put(fullPath, body, false, nil); err != nil {
			return storagedriver.InvalidPathError{Path: w.key}
		}

		fi, _ := w.driver.Client.GetInfo(fullPath)
		if fi.Size == int64(len(w.readyPart)) {
			break
		}
		time.Sleep(time.Second)
	}
	w.closed = true

	return nil
}

func (w *writer) Commit() error {
	fullPath := w.driver.fullPath(w.key)
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	} else if w.cancelled {
		return fmt.Errorf("already cancelled")
	}
	w.committed = true

	for {
		body := bytes.NewBuffer(w.readyPart)
		if err := w.driver.Client.Mkdir(fullPath[:strings.LastIndex(fullPath, "/")]); err != nil {
			return storagedriver.InvalidPathError{Path: w.key}
		}
		if _, err := w.driver.Client.Put(fullPath, body, false, nil); err != nil {
			return storagedriver.InvalidPathError{Path: w.key}
		}
		fi, _ := w.driver.Client.GetInfo(fullPath)
		if fi.Size == int64(len(w.readyPart)) {
			break
		}
		time.Sleep(time.Second)
	}

	return nil
}

func (w *writer) Size() int64 {
	return w.size
}

func (w *writer) Write(p []byte) (int, error) {
	w.readyPart = append(w.readyPart, p...)
	w.size += int64(len(p))

	return len(p), nil
}
