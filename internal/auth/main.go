package auth

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/guppy/pkg/client"
	guppyDelegation "github.com/storacha/guppy/pkg/delegation"
)

// CachedClient holds a client with its creation time for expiration management
type CachedClient struct {
	Client    *client.Client
	CreatedAt time.Time
}

// ClientCache manages cached authenticated clients with thread safety and expiration
type ClientCache struct {
	clients map[string]*CachedClient
	mu      sync.RWMutex
	ttl     time.Duration
}

// Global client cache instance
var (
	clientCache *ClientCache
	once        sync.Once
)

// DefaultCacheTTL is the default time-to-live for cached clients (1 hour)
const DefaultCacheTTL = 1 * time.Hour

// getClientCache returns the singleton client cache instance
func getClientCache() *ClientCache {
	once.Do(func() {
		clientCache = &ClientCache{
			clients: make(map[string]*CachedClient),
			ttl:     DefaultCacheTTL,
		}
	})
	return clientCache
}

// Get retrieves a client from cache if it exists and hasn't expired
func (c *ClientCache) Get(key string) (*client.Client, bool) {
	c.mu.RLock()
	cached, exists := c.clients[key]
	c.mu.RUnlock()

	if !exists {
		return nil, false
	}

	// Check if client has expired
	if time.Since(cached.CreatedAt) > c.ttl {
		// Remove expired client immediately
		c.mu.Lock()
		delete(c.clients, key)
		c.mu.Unlock()
		log.Printf("Client cache: expired entry for key %s (removed)", key)
		return nil, false
	}

	log.Printf("Client cache: hit for key %s", key)
	return cached.Client, true
}

// Set stores a client in cache with current timestamp
func (c *ClientCache) Set(key string, client *client.Client) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.clients[key] = &CachedClient{
		Client:    client,
		CreatedAt: time.Now(),
	}
	log.Printf("Client cache: stored client for key %s", key)

	// Clean up expired entries while we have the lock
	c.cleanupExpiredLocked()
}

// Delete removes a specific client from cache
func (c *ClientCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.clients[key]; exists {
		delete(c.clients, key)
		log.Printf("Client cache: deleted client for key %s", key)
	}
}

// Clear removes all clients from cache
func (c *ClientCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	count := len(c.clients)
	c.clients = make(map[string]*CachedClient)
	log.Printf("Client cache: cleared %d cached clients", count)
}

// cleanupExpiredLocked removes expired entries (must be called with write lock held)
func (c *ClientCache) cleanupExpiredLocked() {
	now := time.Now()
	expiredKeys := make([]string, 0)

	for key, cached := range c.clients {
		if now.Sub(cached.CreatedAt) > c.ttl {
			expiredKeys = append(expiredKeys, key)
		}
	}

	for _, key := range expiredKeys {
		delete(c.clients, key)
	}

	if len(expiredKeys) > 0 {
		log.Printf("Client cache: cleaned up %d expired entries", len(expiredKeys))
	}
}

// Stats returns cache statistics
func (c *ClientCache) Stats() (total int, ttl time.Duration) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.clients), c.ttl
}

// SetTTL updates the cache TTL
func (c *ClientCache) SetTTL(ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.ttl = ttl
	log.Printf("Client cache: TTL updated to %v", ttl)
}

// Legacy global cache variable for backward compatibility
// Deprecated: Use the new ClientCache methods instead
// Note: This map is not thread-safe. It is maintained for backward compatibility
// but access is protected by updating it only within thread-safe ClientCache operations.
var CachedClients = make(map[string]*client.Client)
var legacyCacheMu sync.Mutex

// EmailAuth authenticates using email with caching support
func EmailAuth(email string) (*client.Client, error) {
	cache := getClientCache()

	// Try to get from cache first
	if client, found := cache.Get(email); found {
		return client, nil
	}

	// Not in cache or expired, authenticate
	client, err := emailAuth(email)
	if err != nil {
		return nil, err
	}

	// Store in cache
	cache.Set(email, client)

	// Also update legacy cache for backward compatibility
	legacyCacheMu.Lock()
	CachedClients[email] = client
	legacyCacheMu.Unlock()

	return client, nil
}

func emailAuth(email string) (*client.Client, error) {
	ctx := context.Background()

	parts := strings.Split(email, "@")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid email: %s", email)
	}
	emailUser, emailDomain := parts[0], parts[1]

	account, err := did.Parse("did:mailto:" + emailDomain + ":" + emailUser)
	if err != nil {
		return nil, err
	}

	c, _ := client.NewClient()

	authOk, err := c.RequestAccess(ctx, account.String())
	if err != nil {
		return nil, err
	}

	resultChan := c.PollClaim(ctx, authOk)
	fmt.Println("Please click the link in your email to authenticate...")
	proofs, err := result.Unwrap(<-resultChan)
	if err != nil {
		return nil, err
	}

	if err := c.AddProofs(proofs...); err != nil {
		return nil, fmt.Errorf("failed to add proofs: %w", err)
	}

	return c, nil
}

// AuthConfig holds the private key authentication configuration
type AuthConfig struct {
	PrivateKeyPath string
	ProofPath      string
	SpaceDID       string
}

// PrivateKeyAuth creates an authenticated client using private key + proofs
func PrivateKeyAuth(config *AuthConfig) (*client.Client, error) {
	cacheKey := fmt.Sprintf("pk:%s:%s:%s", config.PrivateKeyPath, config.ProofPath, config.SpaceDID)
	cache := getClientCache()

	// Try to get from cache first
	if client, found := cache.Get(cacheKey); found {
		return client, nil
	}

	issuer, err := loadPrivateKey(config.PrivateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load private key: %w", err)
	}

	proofs, err := loadProofs(config.ProofPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load proofs: %w", err)
	}

	spaceDID, err := did.Parse(config.SpaceDID)
	if err != nil {
		return nil, fmt.Errorf("invalid space DID: %w", err)
	}

	c, err := client.NewClient(client.WithPrincipal(issuer))
	if err != nil {
		return nil, fmt.Errorf("failed to create client with principal: %w", err)
	}

	// Add proofs to the client
	if err := c.AddProofs(proofs...); err != nil {
		return nil, fmt.Errorf("failed to add proofs to client: %w", err)
	}

	// Store in cache
	cache.Set(cacheKey, c)

	// Also update legacy cache for backward compatibility
	legacyCacheMu.Lock()
	CachedClients[cacheKey] = c
	legacyCacheMu.Unlock()

	// issuer implements principal.Signer so we can call DID() on it
	fmt.Printf("✓ Authenticated with private key DID: %s\n", issuer.DID().String())
	fmt.Printf("✓ Using space: %s\n", spaceDID.String())

	return c, nil
}

// PrivateKeyAuthSimple with file paths directly
func PrivateKeyAuthSimple(privateKeyPath, proofPath, spaceDID string) (*client.Client, error) {
	config := &AuthConfig{
		PrivateKeyPath: privateKeyPath,
		ProofPath:      proofPath,
		SpaceDID:       spaceDID,
	}
	return PrivateKeyAuth(config)
}

func loadPrivateKey(privateKeyPath string) (principal.Signer, error) {
	if privateKeyPath == "" {
		return nil, fmt.Errorf("private key path is empty")
	}
	if privateKeyPath[0] == '~' {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("failed to get home directory: %w", err)
		}
		privateKeyPath = filepath.Join(homeDir, privateKeyPath[1:])
	}

	keyData, err := os.ReadFile(privateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read private key file '%s': %w", privateKeyPath, err)
	}

	keyString := strings.TrimSpace(string(keyData))

	// decoding base64 private key (getting issue here)
	keybytes, err := base64.StdEncoding.DecodeString(keyString)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64 private key: %w", err)
	}

	issuer, err := signer.FromRaw(keybytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	fmt.Printf("Successfully parsed private key\n")
	return issuer, nil
}

func loadProofs(proofPath string) ([]delegation.Delegation, error) {
	// Expand home directory if needed
	if proofPath == "" {
		return nil, fmt.Errorf("proof path is empty")
	}
	if proofPath[0] == '~' {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("failed to get home directory: %w", err)
		}
		proofPath = filepath.Join(homeDir, proofPath[1:])
	}

	prfbytes, err := os.ReadFile(proofPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read proof file '%s': %w", proofPath, err)
	}

	proof, err := guppyDelegation.ExtractProof(prfbytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proof: %w", err)
	}

	return []delegation.Delegation{proof}, nil
}

// LoadAuthConfigFromFlags creates auth config from command line parameters
func LoadAuthConfigFromFlags(privateKeyPath, proofPath, spaceDID string) *AuthConfig {
	return &AuthConfig{
		PrivateKeyPath: privateKeyPath,
		ProofPath:      proofPath,
		SpaceDID:       spaceDID,
	}
}

// LoadAuthConfigFromEnv creates auth config from environment variables
func LoadAuthConfigFromEnv() (*AuthConfig, error) {
	privateKeyPath := os.Getenv("STORACHA_PRIVATE_KEY_PATH")
	proofPath := os.Getenv("STORACHA_PROOF_PATH")
	spaceDID := os.Getenv("STORACHA_SPACE_DID")

	if privateKeyPath == "" {
		return nil, fmt.Errorf("STORACHA_PRIVATE_KEY_PATH environment variable is required")
	}
	if proofPath == "" {
		return nil, fmt.Errorf("STORACHA_PROOF_PATH environment variable is required")
	}
	if spaceDID == "" {
		return nil, fmt.Errorf("STORACHA_SPACE_DID environment variable is required")
	}

	return &AuthConfig{
		PrivateKeyPath: privateKeyPath,
		ProofPath:      proofPath,
		SpaceDID:       spaceDID,
	}, nil
}

// ValidateAuthConfig validates that all required files exist and are readable
func ValidateAuthConfig(config *AuthConfig) error {
	privateKeyPath := config.PrivateKeyPath
	proofPath := config.ProofPath

	if privateKeyPath == "" {
		return fmt.Errorf("private key path is empty")
	}
	if proofPath == "" {
		return fmt.Errorf("proof path is empty")
	}

	if privateKeyPath[0] == '~' {
		homeDir, err := os.UserHomeDir()
		if err != nil || homeDir == "" {
			return fmt.Errorf("failed to get home directory: %w", err)
		}
		privateKeyPath = filepath.Join(homeDir, privateKeyPath[1:])
	}

	if proofPath[0] == '~' {
		homeDir, err := os.UserHomeDir()
		if err != nil || homeDir == "" {
			return fmt.Errorf("failed to get home directory: %w", err)
		}
		proofPath = filepath.Join(homeDir, proofPath[1:])
	}

	// Validate private key file
	if _, err := os.Stat(privateKeyPath); os.IsNotExist(err) {
		return fmt.Errorf("private key file does not exist: %s", privateKeyPath)
	}

	// Validate proof file
	if _, err := os.Stat(proofPath); os.IsNotExist(err) {
		return fmt.Errorf("proof file does not exist: %s", proofPath)
	}

	// Validate space DID format
	if _, err := did.Parse(config.SpaceDID); err != nil {
		return fmt.Errorf("invalid space DID format: %w", err)
	}

	return nil
}

// GetAuthMethodFromArgs determines which auth method to use based on provided arguments
func GetAuthMethodFromArgs(email, privateKeyPath, proofPath, spaceDID string) (string, error) {
	hasEmail := email != ""
	hasPrivateKey := privateKeyPath != "" && proofPath != "" && spaceDID != ""

	if hasEmail && hasPrivateKey {
		return "", fmt.Errorf("cannot use both email and private key authentication at the same time")
	}

	if hasEmail {
		return "email", nil
	}

	if hasPrivateKey {
		return "private_key", nil
	}

	return "none", nil
}

// Cache management functions for external use

// ClearClientCache clears all cached clients
func ClearClientCache() {
	getClientCache().Clear()
}

// InvalidateClient removes a specific client from cache
func InvalidateClient(key string) {
	getClientCache().Delete(key)
}

// GetCacheStats returns current cache statistics
func GetCacheStats() (total int, ttl time.Duration) {
	return getClientCache().Stats()
}

// SetCacheTTL updates the cache time-to-live duration
func SetCacheTTL(ttl time.Duration) {
	getClientCache().SetTTL(ttl)
}
