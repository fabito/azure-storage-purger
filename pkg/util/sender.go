package util

import (
	"net/http"

	"github.com/Azure/azure-sdk-for-go/storage"
	log "github.com/sirupsen/logrus"
)

// SenderFunc is a method that implements the Sender interface.
type SenderFunc func(c *storage.Client, r *http.Request) (*http.Response, error)

// Send implements the Sender interface on SenderFunc.
func (sf SenderFunc) Send(c *storage.Client, r *http.Request) (*http.Response, error) {
	return sf(c, r)
}

// SenderWithLogging returns a SendDecorator that implements simple before and after logging of the
// request.
func SenderWithLogging(s storage.Sender) storage.Sender {
	return SenderFunc(func(c *storage.Client, r *http.Request) (*http.Response, error) {
		if log.IsLevelEnabled(log.TraceLevel) {
			log.Tracef("Sending %s %s", r.Method, r.URL)
		}
		resp, err := s.Send(c, r)
		if log.IsLevelEnabled(log.TraceLevel) {
			if err != nil {
				log.Tracef("%s %s received error '%v'", r.Method, r.URL, err)
			} else {
				log.Tracef("%s %s received %s", r.Method, r.URL, resp.Status)
			}
			if resp != nil {
				for k, v := range resp.Header {
					log.Tracef("%s=%s", k, v)
				}
			}

			// log.Trace(resp.Body)
		}
		return resp, err
	})
}
