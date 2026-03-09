// Package storacha provides a bridge to the Storacha JS client for uploads
package storacha

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
)

// Bridge manages communication with the Storacha JS client via pipes
type Bridge struct {
	mu      sync.Mutex
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	stdout  io.ReadCloser
	scanner *bufio.Scanner
	debug   bool

	// Storacha configuration
	spaceDID string // Optional space DID
}

// BridgeConfig holds configuration for the Storacha bridge
type BridgeConfig struct {
	SpaceDID string // Optional: space DID for uploads
	Debug    bool
}

// Command types sent to JS client
type Command struct {
	Type    string      `json:"type"`
	Payload interface{} `json:"payload"`
}

// UploadPayload is sent for upload commands
type UploadPayload struct {
	Path     string `json:"path"`     // Local path to file/directory to upload
	SpaceDID string `json:"spaceDid"` // Optional space DID
}

// Response from JS client
type Response struct {
	Success bool   `json:"success"`
	CID     string `json:"cid,omitempty"`
	Error   string `json:"error,omitempty"`
}

// NewBridge creates a new Storacha bridge
func NewBridge(config BridgeConfig) (*Bridge, error) {
	bridge := &Bridge{
		spaceDID: config.SpaceDID,
		debug:    config.Debug,
	}

	if err := bridge.start(); err != nil {
		return nil, err
	}

	return bridge, nil
}

// start launches the JS client process
func (b *Bridge) start() error {
	// Find the JS bridge script
	// Look in common locations
	scriptPath := b.findBridgeScript()
	if scriptPath == "" {
		return fmt.Errorf("storacha bridge script not found. Please ensure 'storacha-bridge.js' is in the same directory as the binary or in ~/.storachafs/")
	}

	if b.debug {
		log.Printf("Starting Storacha bridge: node %s", scriptPath)
	}

	b.cmd = exec.Command("node", scriptPath)

	var err error
	b.stdin, err = b.cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdin pipe: %w", err)
	}

	b.stdout, err = b.cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	// Forward stderr for debugging
	b.cmd.Stderr = os.Stderr

	if err := b.cmd.Start(); err != nil {
		return fmt.Errorf("failed to start JS bridge: %w", err)
	}

	b.scanner = bufio.NewScanner(b.stdout)

	// Wait for ready signal
	if b.scanner.Scan() {
		var resp Response
		if err := json.Unmarshal([]byte(b.scanner.Text()), &resp); err != nil {
			return fmt.Errorf("invalid ready response: %w", err)
		}
		if !resp.Success {
			return fmt.Errorf("bridge failed to initialize: %s", resp.Error)
		}
		if b.debug {
			log.Println("Storacha bridge ready")
		}
	} else {
		if err := b.scanner.Err(); err != nil {
			return fmt.Errorf("failed to read ready signal: %w", err)
		}
		return fmt.Errorf("bridge closed before ready")
	}

	return nil
}

// findBridgeScript looks for the bridge script in common locations
func (b *Bridge) findBridgeScript() string {
	// Check locations in order
	locations := []string{
		"storacha-bridge.js",
		"./storacha-bridge.js",
		filepath.Join(os.Getenv("HOME"), ".storachafs", "storacha-bridge.js"),
	}

	// Also check relative to executable
	if execPath, err := os.Executable(); err == nil {
		execDir := filepath.Dir(execPath)
		locations = append([]string{
			filepath.Join(execDir, "storacha-bridge.js"),
		}, locations...)
	}

	for _, loc := range locations {
		if _, err := os.Stat(loc); err == nil {
			return loc
		}
	}

	return ""
}

// Upload uploads a file or directory to Storacha and returns the CID
func (b *Bridge) Upload(ctx context.Context, localPath string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cmd := Command{
		Type: "upload",
		Payload: UploadPayload{
			Path:     localPath,
			SpaceDID: b.spaceDID,
		},
	}

	// Send command
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return "", fmt.Errorf("failed to marshal command: %w", err)
	}

	if b.debug {
		log.Printf("Sending to bridge: %s", string(cmdBytes))
	}

	if _, err := fmt.Fprintf(b.stdin, "%s\n", cmdBytes); err != nil {
		return "", fmt.Errorf("failed to send command: %w", err)
	}

	// Read response
	if !b.scanner.Scan() {
		if err := b.scanner.Err(); err != nil {
			return "", fmt.Errorf("failed to read response: %w", err)
		}
		return "", fmt.Errorf("bridge closed unexpectedly")
	}

	var resp Response
	if err := json.Unmarshal([]byte(b.scanner.Text()), &resp); err != nil {
		return "", fmt.Errorf("invalid response: %w", err)
	}

	if !resp.Success {
		return "", fmt.Errorf("upload failed: %s", resp.Error)
	}

	if b.debug {
		log.Printf("Upload successful, CID: %s", resp.CID)
	}

	return resp.CID, nil
}

// UploadCID fetches content from IPFS by CID and uploads to Storacha
func (b *Bridge) UploadCID(ctx context.Context, cid string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cmd := Command{
		Type: "upload-cid",
		Payload: map[string]string{
			"cid":      cid,
			"spaceDid": b.spaceDID,
		},
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return "", fmt.Errorf("failed to marshal command: %w", err)
	}

	if b.debug {
		log.Printf("Sending CID upload to bridge: %s", string(cmdBytes))
	}

	if _, err := fmt.Fprintf(b.stdin, "%s\n", cmdBytes); err != nil {
		return "", fmt.Errorf("failed to send command: %w", err)
	}

	if !b.scanner.Scan() {
		if err := b.scanner.Err(); err != nil {
			return "", fmt.Errorf("failed to read response: %w", err)
		}
		return "", fmt.Errorf("bridge closed unexpectedly")
	}

	var resp Response
	if err := json.Unmarshal([]byte(b.scanner.Text()), &resp); err != nil {
		return "", fmt.Errorf("invalid response: %w", err)
	}

	if !resp.Success {
		return "", fmt.Errorf("CID upload failed: %s", resp.Error)
	}

	return resp.CID, nil
}

// Close shuts down the bridge
func (b *Bridge) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.stdin != nil {
		// Send shutdown command
		cmd := Command{Type: "shutdown"}
		cmdBytes, _ := json.Marshal(cmd)
		fmt.Fprintf(b.stdin, "%s\n", cmdBytes)
		b.stdin.Close()
	}

	if b.cmd != nil && b.cmd.Process != nil {
		return b.cmd.Wait()
	}

	return nil
}
