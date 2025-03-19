use miette::{Diagnostic, Result, miette};
use opencue_proto::{
    host::HardwareState,
    rqd::{RunFrame, run_frame},
};
use std::{fs, sync::Arc};
use thiserror::Error;
use tracing::{error, warn};
use uuid::Uuid;

use crate::{config::config::RunnerConfig, servant::rqd_servant::MachineImpl};

use super::{cache::RunningFrameCache, running_frame::RunningFrame};

pub struct FrameManager {
    pub config: RunnerConfig,
    pub frame_cache: Arc<RunningFrameCache>,
    pub machine: Arc<MachineImpl>,
}

impl FrameManager {
    /// Spawns a new frame to be executed on this host.
    ///
    /// This function handles the entire process of validating, preparing, and launching a frame:
    /// - Validates the frame and machine state
    /// - Reserves CPU resources if the frame is hyperthreaded
    /// - Reserves GPU resources if requested
    /// - Creates the user if necessary
    /// - Initializes and launches the frame in a separate thread
    ///
    /// # Arguments
    ///
    /// * `run_frame` - The grpc frame configuration containing all information needed to run the
    ///   job
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the frame was successfully spawned
    /// * `Err(FrameManagerError)` if the frame could not be spawned for any reason
    pub async fn spawn(&self, run_frame: RunFrame) -> Result<(), FrameManagerError> {
        // Validate machine state
        self.validate_grpc_frame(&run_frame)?;
        self.validate_machine_state(run_frame.ignore_nimby).await?;

        // Create user if required. uid and gid ranges have already been verified
        let uid = match run_frame.uid_optional.as_ref().map(|o| match o {
            run_frame::UidOptional::Uid(v) => *v as u32,
        }) {
            Some(uid) => self
                .machine
                .create_user_if_unexisting(&run_frame.user_name, uid, run_frame.gid as u32)
                .await
                .map_err(|err| {
                    FrameManagerError::Aborted(format!(
                        "Not launching, user {}({}:{}) could not be created. {:?}",
                        run_frame.user_name, uid, run_frame.gid, err
                    ))
                })?,
            None => self.config.default_uid,
        };

        // **Attention**: If an error happens between here and spawning a frame, the resources
        // reserved need to be released.
        //
        // Cuebot unfortunatelly uses a hardcoded frame environment variable to signal if
        // a frame is hyperthreaded. Rqd should only reserve cores if a frame is hyperthreaded.
        let hyperthreaded = run_frame
            .environment
            .get("CUE_THREADABLE")
            .map_or(false, |v| v == "1");
        let cpu_list = match hyperthreaded {
            true => {
                // On cuebot, num_cores are multiplied by 100 to account for fractional reservations
                // rqd doesn't follow the same concept
                let cpu_request = run_frame.num_cores as u32 / 100;
                Some(
                    self.machine
                        .reserve_cores(cpu_request, run_frame.resource_id())
                        .await
                        .map_err(|err| {
                            FrameManagerError::Aborted(format!(
                                "Not launching, failed to reserve cpu resources {:?}",
                                err
                            ))
                        })?,
                )
            }
            false => None,
        };

        // Although num_gpus is not required on a frame, the field is not optional on the proto
        // layer. =0 means None, !=0 means Some
        let gpu_list = match run_frame.num_gpus {
            0 => None,
            _ => {
                let reserved_res = self.machine.reserve_gpus(run_frame.num_gpus as u32).await;
                if let Err(_) = reserved_res {
                    // Release cores reserved on the last step
                    if let Some(procs) = &cpu_list {
                        self.machine.release_cpus(procs).await;
                    }
                }
                Some(reserved_res.map_err(|err| {
                    FrameManagerError::Aborted(format!(
                        "Not launching, insufficient resources {:?}",
                        err
                    ))
                })?)
            }
        };

        let running_frame = Arc::new(RunningFrame::init(
            run_frame,
            uid,
            self.config.clone(),
            cpu_list,
            gpu_list,
            self.machine.get_host_name().await,
        ));

        self.spawn_running_frame(running_frame, false);
        Ok(())
    }

    /// Recovers frames from saved snapshots on disk.
    ///
    /// This function:
    /// - Reads saved frame snapshots from the configured snapshots directory
    /// - Deserializes each valid snapshot file into a RunningFrame
    /// - Spawns recovered frames to continue execution
    /// - Cleans up invalid or corrupted snapshot files
    ///
    /// # Returns
    ///
    /// * `Ok(())` if snapshot recovery was attempted (even if some snapshots failed)
    /// * `Err(miette::Error)` if the snapshots directory could not be read
    pub async fn recover_snapshots(&self) -> Result<()> {
        let snapshots_path = &self.config.snapshots_path;
        let read_dirs = std::fs::read_dir(snapshots_path).map_err(|err| {
            let msg = format!("Failed to read snapshot dir. {}", err);
            warn!(msg);
            miette!(msg)
        })?;
        // Filter paths that are files ending with .bin
        let snapshot_dir: Vec<String> = read_dirs
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let file_type = entry.file_type().ok()?;
                if file_type.is_file() && entry.file_name().to_str().unwrap_or("").ends_with(".bin")
                {
                    entry.path().to_str().map(String::from)
                } else {
                    None
                }
            })
            .collect();
        let mut errors = Vec::new();
        for path in snapshot_dir {
            let running_frame =
                RunningFrame::from_snapshot(&path, self.config.clone()).map(|rf| Arc::new(rf));
            match running_frame {
                Ok(running_frame) => {
                    // Update reservations:
                    if let Some(cpu_list) = &running_frame.cpu_list {
                        if let Err(err) = self
                            .machine
                            .reserve_cores_by_id(cpu_list, running_frame.request.resource_id())
                            .await
                        {
                            errors.push(err.to_string());
                        }
                    }
                    self.spawn_running_frame(running_frame, true)
                }
                Err(err) => {
                    error!("Snapshot recover failed: {}", err);
                    if let Err(err) = fs::remove_file(&path) {
                        warn!("Snapshot {} failed to be cleared. {}", path, err);
                    };
                }
            }
        }

        if !errors.is_empty() {
            Err(miette!("{}", errors.join("\n")))
        } else {
            Ok(())
        }
    }

    fn spawn_running_frame(&self, running_frame: Arc<RunningFrame>, recover_mode: bool) {
        self.frame_cache
            .insert_running_frame(Arc::clone(&running_frame));
        let running_frame_ref: Arc<RunningFrame> = Arc::clone(&running_frame);
        // Fire and forget
        let thread_handle = std::thread::spawn(move || {
            let result = std::panic::catch_unwind(|| running_frame.run(recover_mode));
            if let Err(panic_info) = result {
                _ = running_frame.finish(1, None);
                error!(
                    "Run thread panicked for {}: {:?}",
                    running_frame, panic_info
                );
            }
        });
        if let Err(err) = running_frame_ref.update_launch_thread_handle(thread_handle) {
            warn!(
                "Failed to update thread handle for frame {}. {}",
                running_frame_ref, err
            );
        }
    }

    fn validate_grpc_frame(&self, run_frame: &RunFrame) -> Result<(), FrameManagerError> {
        // Frame is already running
        if self.frame_cache.contains(&run_frame.frame_id()) {
            Err(FrameManagerError::AlreadyExist(format!(
                "Not lauching, frame is already running on this host {}",
                run_frame.frame_id()
            )))?
        }
        // Trying to run as root
        if run_frame
            .uid_optional
            .clone()
            .map(|o| match o {
                run_frame::UidOptional::Uid(v) => v,
            })
            .unwrap_or(1)
            <= 0
        {
            Err(FrameManagerError::InvalidArgument(format!(
                "Not launching, will not run frame as uid = {:?}",
                run_frame.uid_optional
            )))?
        }
        // Invalid number of cores
        if run_frame.num_cores <= 0 {
            Err(FrameManagerError::InvalidArgument(
                "Not launching, num_cores must be positive".to_string(),
            ))?
        }
        // Fractional cpu cores are not allowed
        if run_frame.num_cores % 100 != 0 {
            Err(FrameManagerError::InvalidArgument(
                "Not launching, num_cores must be multiple of 100 (Fractional Cores are not allowed)".to_string(),
            ))?
        }

        Ok(())
    }

    async fn validate_machine_state(&self, ignore_nimby: bool) -> Result<(), FrameManagerError> {
        // Hardware state is not UP
        if self
            .machine
            .hardware_state()
            .await
            .unwrap_or(HardwareState::Down)
            != HardwareState::Up
        {
            Err(FrameManagerError::InvalidHardwareState(
                "Not launching, host HardwareState is not Up".to_string(),
            ))?
        }
        // Nimby locked
        if self.machine.nimby_locked().await && !ignore_nimby {
            Err(FrameManagerError::NimbyLocked)?
        }
        Ok(())
    }
}

#[derive(Debug, Error, Diagnostic)]
pub enum FrameManagerError {
    #[error("Action aborted due to a host invalid state")]
    InvalidHardwareState(String),
    #[error("Invalid Request")]
    InvalidArgument(String),
    #[error("Frame is already running")]
    AlreadyExist(String),
    #[error("Aborted")]
    Aborted(String),
    #[error("Execution aborted, host is nimby locked")]
    NimbyLocked,
}

impl From<FrameManagerError> for tonic::Status {
    fn from(value: FrameManagerError) -> Self {
        match value {
            FrameManagerError::InvalidHardwareState(msg) => tonic::Status::failed_precondition(msg),
            FrameManagerError::InvalidArgument(msg) => tonic::Status::invalid_argument(msg),
            FrameManagerError::AlreadyExist(msg) => tonic::Status::invalid_argument(msg),
            FrameManagerError::Aborted(msg) => tonic::Status::aborted(msg),
            FrameManagerError::NimbyLocked => tonic::Status::aborted("Nimby Locked"),
        }
    }
}
