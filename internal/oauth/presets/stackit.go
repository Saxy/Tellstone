/*
Package presets
Tellstone OAuth Provider Presets
File: stackit.go
Description: STACKIT identity provider preset.

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

// stackitIssuer is STACKIT IAM's OIDC issuer, taken from its discovery document
// at https://accounts.stackit.cloud/.well-known/openid-configuration.
const stackitIssuer = "https://accounts.stackit.cloud"

// stackitScopes are the scopes for which STACKIT IAM issues an id_token.
var stackitScopes = []string{"openid", "email", "profile"}

type stackit struct {
	inner *generic.Provider
	cfg   oauth.Config
}

func NewStackit(cfg oauth.Config, logger log.Logger) (*stackit, error) {
	if cfg.Issuer == "" {
		cfg.Issuer = stackitIssuer
	}
	if len(cfg.Scopes) == 0 {
		cfg.Scopes = stackitScopes
	}
	inner, err := generic.New(cfg, logger)
	if err != nil {
		return nil, err
	}
	return &stackit{inner: inner, cfg: cfg}, nil
}

func (s *stackit) Config() oauth.Config { return s.cfg }

func (s *stackit) Verify(ctx context.Context, token []byte) (oauth.Claims, error) {
	return s.inner.Verify(ctx, token)
}
