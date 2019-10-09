/// Configuration Options

use std::path::Path;

pub const LINE_WIDTH: usize = 75;
pub const TRACE_CACHE: &Path = Path::new("/opt/stack/pythia_trace_cache");
pub const REDIS_URL: &str = "redis://localhost:6379";
pub enum ManifestMethod {
    CCT,
    Poset
}
pub const MANIFEST_METHOD: ManifestMethod = ManifestMethod::CCT;
pub const MANIFEST_ROOT: &Path = Path::new("/opt/stack/manifest");
