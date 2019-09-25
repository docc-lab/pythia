/// Configuration Options
pub const LINE_WIDTH: usize = 75;
pub const TRACE_CACHE: &str = "/opt/stack/pythia_trace_cache/";
pub const REDIS_URL: &str = "redis://localhost:6379";
pub enum ManifestMethod {
    CCT,
    Poset
}
pub const MANIFEST_METHOD: ManifestMethod = ManifestMethod::CCT;
