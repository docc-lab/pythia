/*
This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.

Copyright (c) 2022, Diagnosis and Control of Clouds Laboratory
All rights reserved.
*/

//! The code shared between the two Pythia projects. Mostly
//! type definitions.

#[macro_use]
extern crate lazy_static;

mod budget;
pub mod osprofiler;

use std::error::Error;
use std::fmt;

pub use crate::osprofiler::AnnotationEnum;
pub use crate::osprofiler::OSProfilerEnum;
pub use crate::osprofiler::OSProfilerSpan;
pub use crate::osprofiler::RequestType;
pub use crate::osprofiler::REQUEST_TYPES;
pub use crate::osprofiler::REQUEST_TYPE_REGEXES;

pub use crate::budget::NodeStats;

/// Error raised from within Pythia. It just has a String error message.
///
/// Rust requires everyone to have their own error type.
#[derive(Debug)]
pub struct PythiaError(String);

impl fmt::Display for PythiaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Pythia error: {}", self.0)
    }
}

impl Error for PythiaError {}
