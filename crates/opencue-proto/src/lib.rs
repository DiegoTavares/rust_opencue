use core::fmt;
use std::path::Display;

use host::Host;
use job::{Frame, Job};
use rqd::RunFrame;
use uuid::Uuid;

mod comment;
mod criterion;
mod cue;
mod department;
mod depend;
pub mod facility;
mod filter;
pub mod host;
pub mod job;
mod limit;
mod render_partition;
pub mod report;
pub mod rqd;
mod service;
pub mod show;
mod subscription;
mod task;
pub mod test_utils;

pub trait WithUuid {
    fn uuid(&self) -> Uuid;
}

impl WithUuid for job::Job {
    fn uuid(&self) -> Uuid {
        to_uuid(&self.id).expect("Failed to convert Uuid, protocol version is incompatible")
    }
}

impl WithUuid for job::Layer {
    fn uuid(&self) -> Uuid {
        to_uuid(&self.id).expect("Failed to convert Uuid, protocol version is incompatible")
    }
}

impl WithUuid for facility::Allocation {
    fn uuid(&self) -> Uuid {
        to_uuid(&self.id).expect("Failed to convert Uuid, protocol version is incompatible")
    }
}

impl WithUuid for show::Show {
    fn uuid(&self) -> Uuid {
        to_uuid(&self.id).expect("Failed to convert Uuid, protocol version is incompatible")
    }
}

impl WithUuid for report::RunningFrameInfo {
    fn uuid(&self) -> Uuid {
        to_uuid(&self.frame_id).expect("Failed to convert Uuid, protocol version is incompatible")
    }
}

pub fn to_uuid(stringified_id_from_protobuf: &str) -> Option<Uuid> {
    Uuid::parse_str(stringified_id_from_protobuf).ok()
}

impl RunFrame {
    pub fn job_id(&self) -> Uuid {
        to_uuid(&self.job_id).expect("Failed to convert Uuid, protocol version is incompatible")
    }
    pub fn frame_id(&self) -> Uuid {
        to_uuid(&self.frame_id).expect("Failed to convert Uuid, protocol version is incompatible")
    }
    pub fn layer_id(&self) -> Uuid {
        to_uuid(&self.layer_id).expect("Failed to convert Uuid, protocol version is incompatible")
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:}/({:})", self.name, self.id)
    }
}

impl fmt::Display for Job {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:}/({:})", self.name, self.id)
    }
}

impl fmt::Display for Host {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:}/({:})", self.name, self.id)
    }
}
