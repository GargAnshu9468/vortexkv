//go:build !darwin && !linux

package reactor

// NewServer instantiates the fallback netpoller reactor on non-UNIX platforms.
func NewServer(cfg Config) (Server, error) {
	return NewFallbackReactor(cfg)
}
