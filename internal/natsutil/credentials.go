package natsutil

import (
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func JWTAuth(token, seed string) nats.Option {
	return nats.UserJWT(
		func() (string, error) { return token, nil },
		func(nonce []byte) ([]byte, error) {
			kp, err := nkeys.FromSeed([]byte(seed))
			if err != nil {
				return nil, err
			}
			return kp.Sign(nonce)
		},
	)
}

func CredsFileAuth(credsPath string) nats.Option {
	return nats.UserCredentials(credsPath)
}

func CAPemAuth(caPemPath string) nats.Option {
	if caPemPath == "" {
		return func(_ *nats.Options) error { return nil }
	}

	return nats.RootCAs(caPemPath)
}
