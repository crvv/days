package utility

import (
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/jlaffaye/ftp"
)

func checkLocal(u string) ([]byte, string) {
	path := strings.Split(u, "/")
	basename := path[len(path)-1]
	d, err := ioutil.ReadFile(basename)
	if err != nil {
		return nil, basename
	}
	return d, basename
}

func Download(u string) []byte {
	data, basename := checkLocal(u)
	if data != nil {
		log.Println("read local file")
		return data
	}
	log.Println("download", u)
	url1, err := url.Parse(u)
	if err != nil {
		log.Fatal(err)
	}
	switch url1.Scheme {
	case "http":
		data = downloadHTTP(u)
	case "ftp":
		data = downloadFTP(url1)
	default:
		log.Fatalf("URL %v is not supported", u)
	}
	log.Println("completed")
	_ = ioutil.WriteFile(basename, data, 0400)
	return data
}

func downloadHTTP(u string) []byte {
	response, err := http.Get(u)
	if err != nil {
		log.Fatal(err)
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Fatal(err)
	}
	return body
}

func downloadFTP(url1 *url.URL) []byte {
	port := url1.Port()
	if port == "" {
		port = "21"
	}
	conn, err := ftp.Dial(url1.Hostname() + ":" + port)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Quit()
	err = conn.Login("anonymous", "")
	if err != nil {
		log.Fatal(err)
	}
	response, err := conn.Retr(url1.Path)
	if err != nil {
		log.Fatal(err)
	}
	defer response.Close()
	body, err := ioutil.ReadAll(response)
	if err != nil {
		log.Fatal(err)
	}
	return body
}
