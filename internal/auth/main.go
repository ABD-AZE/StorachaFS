package auth

import (
	"context"
	"fmt"
	"strings"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/client"
)

func EmailAuth(email string) (*client.Client, error) {
	cachedClients := make(map[string]*client.Client)
	if _, ok := cachedClients[email]; ok {
		return cachedClients[email], nil
	}
	cachedClients[email], _ = emailAuth(email)
	return cachedClients[email], nil
}

func emailAuth(email string) (*client.Client, error) {
	ctx := context.Background()

	emailDomain := strings.Split(email, "@")[1]
	emailUser := strings.Split(email, "@")[0]
	account, err := did.Parse("did:mailto:" + emailDomain + ":" + emailUser)
	if err != nil {
		return nil, err
	}

	c, _ := client.NewClient()

	// Kick off the login flow
	authOk, err := c.RequestAccess(ctx, account.String())
	if err != nil {
		return nil, err
	}

	// Start polling to see if the user has authenticated yet
	resultChan := c.PollClaim(ctx, authOk)
	fmt.Println("Please click the link in your email to authenticate...")
	// Wait for the user to authenticate
	proofs, err := result.Unwrap(<-resultChan)
	if err != nil {
		return nil, err
	}

	if err := c.AddProofs(proofs...); err != nil {
		return nil, fmt.Errorf("failed to add proofs: %w", err)
	}

	return c, nil
}
