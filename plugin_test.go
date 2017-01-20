package caddytlss3

import (
	"encoding/hex"
	"io"
	"log"
	"math/rand"
	"net/url"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/mholt/caddy/caddytls"
)

var rnd *rand.Rand

func init() {
	rnd = rand.New(rand.NewSource(time.Now().UnixNano()))
}

func newTestStorage(t *testing.T) caddytls.Storage {
	bucket := os.Getenv("TEST_S3_BUCKET")
	if bucket == "" {
		t.Skip("TEST_S3_BUCKET environment variable not set.")
	}
	prefix := randomPrefix(t)
	ur, err := url.Parse("s3://" + bucket + "/" + prefix)
	if err != nil {
		log.Fatal(err)
	}
	storage, err := NewS3Storage(ur)
	if err != nil {
		t.Fatal(err)
	}
	return storage
}

func TestS3StorageIntegrationDomain(t *testing.T) {
	storage := newTestStorage(t)

	domain := "example.com"

	exists, err := storage.SiteExists(domain)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Errorf("Expected exists to be false, got true")
	}

	_, err = storage.LoadSite(domain)
	if err == nil {
		t.Error("Expected site not to exist")
	} else if _, ok := err.(caddytls.ErrNotExist); !ok {
		t.Errorf("Expected caddytls.ErrNotExist, got %T", err)
	}

	siteData := &caddytls.SiteData{
		Cert: []byte("cert"),
		Key:  []byte("key"),
		Meta: []byte("meta"),
	}
	if err := storage.StoreSite(domain, siteData); err != nil {
		t.Fatal(err)
	}
	defer storage.DeleteSite(domain)

	sd, err := storage.LoadSite(domain)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sd, siteData) {
		t.Errorf("Expected %#+v for site data got %#+v", siteData, sd)
	}

	if err := storage.DeleteSite(domain); err != nil {
		t.Fatal(err)
	}

	exists, err = storage.SiteExists(domain)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Errorf("Expected exists to be false, got true")
	}

	_, err = storage.LoadSite(domain)
	if err == nil {
		t.Error("Expected site not to exist")
	} else if _, ok := err.(caddytls.ErrNotExist); !ok {
		t.Errorf("Expected caddytls.ErrNotExist, got %T", err)
	}
}

func TestS3StorageIntegrationUser(t *testing.T) {
	storage := newTestStorage(t)

	email := "someone@example.com"

	if user := storage.MostRecentUserEmail(); user != "" {
		t.Errorf("Expected no most recent user, got %q", user)
	}

	_, err := storage.LoadUser(email)
	if err == nil {
		t.Error("Expected user not to exist")
	} else if _, ok := err.(caddytls.ErrNotExist); !ok {
		t.Errorf("Expected caddytls.ErrNotExist, got %T", err)
	}

	userData := &caddytls.UserData{
		Reg: []byte("reg"),
		Key: []byte("key"),
	}
	if err := storage.StoreUser(email, userData); err != nil {
		t.Fatal(err)
	}
	// TODO: delete the create user data for cleanup

	ud, err := storage.LoadUser(email)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ud, userData) {
		t.Errorf("Expected %#+v for user data got %#+v", userData, ud)
	}

	if user := storage.MostRecentUserEmail(); user != email {
		t.Errorf("Expected %q for most recent user, got %q", email, user)
	}
}

func randomPrefix(t *testing.T) string {
	var b [16]byte
	if _, err := io.ReadFull(rnd, b[:]); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b[:])
}
