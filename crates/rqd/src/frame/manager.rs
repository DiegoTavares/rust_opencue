use miette::Result;
use opencue_proto::{
    host::HardwareState,
    rqd::{RunFrame, run_frame},
};
use std::sync::Arc;
use tracing::error;

use crate::{config::config::Config, servant::rqd_servant::MachineImpl};

use super::{cache::RunningFrameCache, running_frame::RunningFrame};

pub struct FrameManager {
    pub config: Config,
    pub frame_cache: Arc<RunningFrameCache>,
    pub machine: Arc<MachineImpl>,
}

impl FrameManager {
    pub async fn spawn(&self, run_frame: RunFrame) -> Result<(), FrameManagerError> {
        // Validate machine state
        self.validate_grpc_frame(&run_frame)?;
        self.validate_machine_state(run_frame.ignore_nimby).await?;

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
                        .reserve_cpus(cpu_request)
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
            _ => Some(
                self.machine
                    .reserve_gpus(run_frame.num_gpus as u32)
                    .await
                    .map_err(|err| {
                        FrameManagerError::Aborted(format!(
                            "Not launching, insufficient resources {:?}",
                            err
                        ))
                    })?,
            ),
        };

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
            None => self.config.runner.default_uid,
        };

        let running_frame = Arc::new(RunningFrame::init(
            run_frame,
            uid,
            self.config.runner.clone(),
            cpu_list,
            gpu_list,
            self.machine.get_host_name().await,
        ));

        self.frame_cache
            .insert_running_frame(Arc::clone(&running_frame));
        let running_frame_ref: Arc<RunningFrame> = Arc::clone(&running_frame);
        // Fire and forget
        let thread_handle = std::thread::spawn(move || {
            let result = std::panic::catch_unwind(|| running_frame.run());
            if let Err(panic_info) = result {
                running_frame.update_exit_code(1);
                error!("Run thread panicked: {:?}", panic_info);
            }
        });
        // Another option would be to use a blocking context from tokio.
        // let _t = tokio::task::spawn_blocking(move || running_frame.run());

        // Store the thread handle for bookeeping
        // TODO: Implement logic that checks if the thread is alive during monitoring
        running_frame_ref.update_launch_thread_handle(thread_handle);
        Ok(())
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
        if self.machine.nimby_locked().await && !ignore_nimby {}
        // TODO: Chech if nimby is active and user activity was detected

        Ok(())
    }
}

pub enum FrameManagerError {
    InvalidHardwareState(String),
    InvalidArgument(String),
    AlreadyExist(String),
    Aborted(String),
}
impl From<FrameManagerError> for tonic::Status {
    fn from(value: FrameManagerError) -> Self {
        match value {
            FrameManagerError::InvalidHardwareState(msg) => tonic::Status::failed_precondition(msg),
            FrameManagerError::InvalidArgument(msg) => tonic::Status::invalid_argument(msg),
            FrameManagerError::AlreadyExist(msg) => tonic::Status::invalid_argument(msg),
            FrameManagerError::Aborted(msg) => tonic::Status::aborted(msg),
        }
    }
}
