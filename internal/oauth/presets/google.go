/*
Package presets
Tellstone OAuth Provider Presets
File: google.go
Description: Google identity provider preset.

Authors:

	Maximilian Hagen
*/
package presets

import (
	"context"

	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/oauth"
	"github.com/Saxy/Tellstone/internal/oauth/generic"
)

// googleIssuer is Google's OIDC issuer as returned by its discovery document.
const googleIssuer = "https://accounts.google.com"

// googleScopes are the scopes for which Google issues an id_token.
var googleScopes = []string{"openid", "email", "profile"}

type google struct {
	inner *generic.Provider
	cfg   oauth.Config
}

func NewGoogle(cfg oauth.Config, logger log.Logger) (*google, error) {
	if cfg.Issuer == "" {
		cfg.Issuer = googleIssuer
	}
	if len(cfg.Scopes) == 0 {
		cfg.Scopes = googleScopes
	}
	inner, err := generic.New(cfg, logger)
	if err != nil {
		return nil, err
	}
	return &google{inner: inner, cfg: cfg}, nil
}

func (g *google) Config() oauth.Config { return g.cfg }

func (g *google) Verify(ctx context.Context, token []byte) (oauth.Claims, error) {
	claims, err := g.inner.Verify(ctx, token)
	if err != nil {
		return nil, err
	}
	if hd, ok := claims["hd"]; ok && len(hd) == 1 && len(claims["groups"]) == 0 {
		claims["groups"] = []string{hd[0]}
	}
	return claims, nil
}
