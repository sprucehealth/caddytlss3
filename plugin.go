package caddytlss3

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/caddyserver/caddy/caddytls"
)

// TODO:
// - support credentials in the config URL
// - distributed locks to avoid generating certs on multiple hosts
// - region support
// - setting bucket without env

func init() {
	// caddy.RegisterPlugin("s3", caddy.Plugin{Action: setup})
	caddytls.RegisterStorageProvider("s3", NewS3Storage)
}

// func setup(c *caddy.Controller) error {
// 	println("XXXXX")
// 	for c.Next() { // skip the directive name
// 		if !c.NextArg() { // expect at least one value
// 			return c.ArgErr() // otherwise it's an error
// 		}
// 		value := c.Val() // use the value
// 		println(value)
// 	}
// 	return nil
// }

type S3Storage struct {
	bucket      string
	prefix      string
	s3          s3iface.S3API
	nameLocksMu sync.Mutex
	nameLocks   map[string]*sync.WaitGroup
}

// NewS3Storage instantiates a new caddy TLS storage instance that uses S3.
func NewS3Storage(caURL *url.URL) (caddytls.Storage, error) {
	cred := credentials.NewEnvCredentials()
	if v, err := cred.Get(); err != nil || v.AccessKeyID == "" || v.SecretAccessKey == "" {
		cred = ec2rolecreds.NewCredentials(session.New(), func(p *ec2rolecreds.EC2RoleProvider) {
			p.ExpiryWindow = time.Minute * 5
		})
	}
	bucket := os.Getenv("CADDY_S3_BUCKET")
	if bucket == "" {
		return nil, errors.New("CADDY_S3_BUCKET not set")
	}
	session := session.New(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: cred,
	})
	return &S3Storage{
		bucket:    bucket,
		prefix:    "acme/" + caURL.Host + "/",
		s3:        s3.New(session),
		nameLocks: make(map[string]*sync.WaitGroup),
	}, nil
}

func (s *S3Storage) domainKey(domain string) *string {
	domain = strings.ToLower(domain)
	return aws.String(s.prefix + "domain/" + domain)
}

func (s *S3Storage) userKey(email string) *string {
	email = strings.ToLower(email)
	return aws.String(s.prefix + "user/" + email)
}

// TryLock attempts to get a lock for name, otherwise it returns
// a Waiter value to wait until the other process is finished.
func (s *S3Storage) TryLock(name string) (caddytls.Waiter, error) {
	s.nameLocksMu.Lock()
	defer s.nameLocksMu.Unlock()
	wg, ok := s.nameLocks[name]
	if ok {
		// lock already obtained, let caller wait on it
		return wg, nil
	}
	// caller gets lock
	wg = new(sync.WaitGroup)
	wg.Add(1)
	s.nameLocks[name] = wg
	return nil, nil
}

// Unlock unlocks name.
func (s *S3Storage) Unlock(name string) error {
	s.nameLocksMu.Lock()
	defer s.nameLocksMu.Unlock()
	wg, ok := s.nameLocks[name]
	if !ok {
		return fmt.Errorf("S3Storage: no lock to release for %s", name)
	}
	wg.Done()
	delete(s.nameLocks, name)
	return nil
}

// SiteExists returns true if this site exists in storage.
// Site data is considered present when StoreSite has been called
// successfully (without DeleteSite having been called, of course).
func (s *S3Storage) SiteExists(domain string) (bool, error) {
	_, err := s.s3.HeadObject(&s3.HeadObjectInput{
		Bucket: &s.bucket,
		Key:    s.domainKey(domain),
	})
	if err != nil {
		if e, ok := err.(awserr.RequestFailure); ok && e.StatusCode() == http.StatusNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// LoadSite obtains the site data from storage for the given domain and
// returns it. If data for the domain does not exist, an error value
// of type ErrNotExist is returned. For multi-server storage, care
// should be taken to make this load atomic to prevent race conditions
// that happen with multiple data loads.
func (s *S3Storage) LoadSite(domain string) (*caddytls.SiteData, error) {
	res, err := s.s3.GetObject(&s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    s.domainKey(domain),
	})
	if err != nil {
		if e, ok := err.(awserr.RequestFailure); ok && e.StatusCode() == http.StatusNotFound {
			return nil, caddytls.ErrNotExist(err)
		}
		return nil, err
	}
	defer res.Body.Close()
	var data *caddytls.SiteData
	if err := json.NewDecoder(res.Body).Decode(&data); err != nil {
		return nil, err
	}
	return data, nil
}

// StoreSite persists the given site data for the given domain in
// storage. For multi-server storage, care should be taken to make this
// call atomic to prevent half-written data on failure of an internal
// intermediate storage step. Implementers can trust that at runtime
// this function will only be invoked after LockRegister and before
// UnlockRegister of the same domain.
func (s *S3Storage) StoreSite(domain string, data *caddytls.SiteData) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = s.s3.PutObject(&s3.PutObjectInput{
		Bucket:               &s.bucket,
		Key:                  s.domainKey(domain),
		Body:                 bytes.NewReader(jsonData),
		ContentLength:        aws.Int64(int64(len(jsonData))),
		ServerSideEncryption: aws.String("AES256"),
	})
	return err
}

// DeleteSite deletes the site for the given domain from storage.
// Multi-server implementations should attempt to make this atomic. If
// the site does not exist, an error value of type ErrNotExist is returned.
func (s *S3Storage) DeleteSite(domain string) error {
	_, err := s.s3.DeleteObject(&s3.DeleteObjectInput{
		Bucket: &s.bucket,
		Key:    s.domainKey(domain),
	})
	return err
}

// LoadUser obtains user data from storage for the given email and
// returns it. If data for the email does not exist, an error value
// of type ErrNotExist is returned. Multi-server implementations
// should take care to make this operation atomic for all loaded
// data items.
func (s *S3Storage) LoadUser(email string) (*caddytls.UserData, error) {
	res, err := s.s3.GetObject(&s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    s.userKey(email),
	})
	if err != nil {
		if e, ok := err.(awserr.RequestFailure); ok && e.StatusCode() == http.StatusNotFound {
			return nil, caddytls.ErrNotExist(err)
		}
		return nil, err
	}
	defer res.Body.Close()
	var data *caddytls.UserData
	if err := json.NewDecoder(res.Body).Decode(&data); err != nil {
		return nil, err
	}
	return data, nil
}

// StoreUser persists the given user data for the given email in
// storage. Multi-server implementations should take care to make this
// operation atomic for all stored data items.
func (s *S3Storage) StoreUser(email string, data *caddytls.UserData) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = s.s3.PutObject(&s3.PutObjectInput{
		Bucket:               &s.bucket,
		Key:                  s.userKey(email),
		Body:                 bytes.NewReader(jsonData),
		ContentLength:        aws.Int64(int64(len(jsonData))),
		ServerSideEncryption: aws.String("AES256"),
	})
	if err != nil {
		return err
	}
	// Store most recent user
	_, err = s.s3.PutObject(&s3.PutObjectInput{
		Bucket:               &s.bucket,
		Key:                  s.userKey("recent"),
		Body:                 strings.NewReader(email),
		ContentLength:        aws.Int64(int64(len(email))),
		ServerSideEncryption: aws.String("AES256"),
	})
	return err
}

// MostRecentUserEmail provides the most recently used email parameter
// in StoreUser. The result is an empty string if there are no
// persisted users in storage.
func (s *S3Storage) MostRecentUserEmail() string {
	res, err := s.s3.GetObject(&s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    s.userKey("recent"),
	})
	if err != nil {
		return ""
	}
	defer res.Body.Close()
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return ""
	}
	return string(b)
}
