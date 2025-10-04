package auth

import (
	"errors"
	"os"
)

// AuthConfig holds authentication configuration
type AuthConfig struct {
	PrivateKeyPath string
	ProofPath      string
	SpaceDID       string
}

// GetAuthMethodFromArgs determines the authentication method from command line arguments
func GetAuthMethodFromArgs(email, privateKeyPath, proofPath, spaceDID string) (string, error) {
	if email != "" {
		return "email", nil
	}
	
	if privateKeyPath != "" || proofPath != "" || spaceDID != "" {
		if privateKeyPath == "" || proofPath == "" || spaceDID == "" {
			return "", errors.New("when using private key authentication, all of --private-key, --proof, and --space are required")
		}
		return "private_key", nil
	}
	
	return "none", nil
}

// LoadAuthConfigFromFlags loads auth config from command line flags
func LoadAuthConfigFromFlags(privateKeyPath, proofPath, spaceDID string) *AuthConfig {
	return &AuthConfig{
		PrivateKeyPath: privateKeyPath,
		ProofPath:      proofPath,
		SpaceDID:       spaceDID,
	}
}

// LoadAuthConfigFromEnv loads auth config from environment variables
func LoadAuthConfigFromEnv() (*AuthConfig, error) {
	privateKeyPath := os.Getenv("STORACHA_PRIVATE_KEY_PATH")
	proofPath := os.Getenv("STORACHA_PROOF_PATH")
	spaceDID := os.Getenv("STORACHA_SPACE_DID")
	
	if privateKeyPath == "" || proofPath == "" || spaceDID == "" {
		return nil, errors.New("missing required environment variables: STORACHA_PRIVATE_KEY_PATH, STORACHA_PROOF_PATH, STORACHA_SPACE_DID")
	}
	
	return &AuthConfig{
		PrivateKeyPath: privateKeyPath,
		ProofPath:      proofPath,
		SpaceDID:       spaceDID,
	}, nil
}

// ValidateAuthConfig validates the auth configuration
func ValidateAuthConfig(config *AuthConfig) error {
	if config == nil {
		return errors.New("auth config is nil")
	}
	
	// Check if private key file exists
	if _, err := os.Stat(config.PrivateKeyPath); os.IsNotExist(err) {
		return errors.New("private key file does not exist: " + config.PrivateKeyPath)
	}
	
	// Check if proof file exists
	if _, err := os.Stat(config.ProofPath); os.IsNotExist(err) {
		return errors.New("proof file does not exist: " + config.ProofPath)
	}
	
	// Basic validation of space DID format
	if config.SpaceDID == "" {
		return errors.New("space DID cannot be empty")
	}
	
	return nil
}