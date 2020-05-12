#[macro_use]
extern crate lazy_static;

mod osprofiler;

pub use crate::osprofiler::AnnotationEnum;
pub use crate::osprofiler::OSProfilerEnum;
pub use crate::osprofiler::OSProfilerSpan;
pub use crate::osprofiler::RequestType;
pub use crate::osprofiler::REQUEST_TYPES;
pub use crate::osprofiler::REQUEST_TYPE_REGEXES;
