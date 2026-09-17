//go:build darwin

package reactor

// NewServer instantiates the Darwin kqueue event reactor by default,
// or the fallback reactor if EngineType is explicitly "std".
func NewServer(cfg Config) (Server, error) {
	if cfg.EngineType == "std" {
		return NewFallbackReactor(cfg)
	}
	return NewDarwinReactor(cfg)
}
