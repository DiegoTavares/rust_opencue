use std::sync::Arc;
use std::time::Duration;

use crate::config::config::Config;
use async_trait::async_trait;
use miette::{IntoDiagnostic, Result};
use opencue_proto::report::{self as pb, rqd_report_interface_client::RqdReportInterfaceClient};
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tower::ServiceBuilder;
use tower::util::rng::HasherRng;

use super::retry::backoff::{ExponentialBackoffMaker, MakeBackoff};
use super::retry::backoff_policy::BackoffPolicy;
use super::retry::{Retry, RetryLayer};

pub(crate) struct ReportClient {
    client: Arc<Mutex<RqdReportInterfaceClient<Retry<BackoffPolicy, Channel>>>>,
}

impl ReportClient {
    pub async fn build(config: &Config) -> Result<Self> {
        let endpoint = format!("http://{}", config.grpc.cuebot_url.clone());

        let channel = tonic::transport::Channel::from_shared(endpoint)
            .into_diagnostic()?
            .connect_lazy();

        // Use a backoff stratery to retry failed requests
        let backoff = ExponentialBackoffMaker::new(
            Duration::from_millis(50),
            Duration::from_secs(300),
            100.0,
            HasherRng::default(),
        )
        .into_diagnostic()?
        .make_backoff();

        // Requests will only return error after 5 attempts
        let retry_policy = BackoffPolicy {
            attempts: None,
            backoff,
        };
        let retry_layer = RetryLayer::new(retry_policy);
        let channel = ServiceBuilder::new().layer(retry_layer).service(channel);

        let client = Arc::new(Mutex::new(RqdReportInterfaceClient::new(channel)));

        // Return the constructed client
        Ok(Self { client })
    }
}

#[async_trait]
pub trait ReportInterface {
    async fn send_start_up_report(
        &self,
        render_host: pb::RenderHost,
        core_detail: pb::CoreDetail,
    ) -> Result<()>;
    async fn send_frame_complete_report(
        &self,
        host: pb::RenderHost,
        frame: pb::RunningFrameInfo,
        exit_status: u32,
        exit_signal: u32,
        run_time: u32,
    ) -> Result<()>;
    async fn send_host_report(
        &self,
        host: pb::RenderHost,
        frames: Vec<pb::RunningFrameInfo>,
        core_info: pb::CoreDetail,
    ) -> Result<()>;
}

#[async_trait]
impl ReportInterface for ReportClient {
    async fn send_start_up_report(
        &self,
        render_host: pb::RenderHost,
        core_detail: pb::CoreDetail,
    ) -> Result<()> {
        let mut client = self.client.lock().await;
        let mut request = pb::RqdReportRqdStartupRequest::default();
        request.boot_report = Some(pb::BootReport {
            host: Some(render_host),
            core_info: Some(core_detail),
        });
        client
            .report_rqd_startup(request)
            .await
            .into_diagnostic()
            .and(Ok(()))
    }

    async fn send_frame_complete_report(
        &self,
        render_host: pb::RenderHost,
        running_frame: pb::RunningFrameInfo,
        exit_status: u32,
        exit_signal: u32,
        run_time: u32,
    ) -> Result<()> {
        let mut client = self.client.lock().await;
        let mut request = pb::RqdReportRunningFrameCompletionRequest::default();
        request.frame_complete_report = Some(pb::FrameCompleteReport {
            host: Some(render_host),
            frame: Some(running_frame),
            exit_status: exit_status as i32,
            exit_signal: exit_signal as i32,
            run_time: run_time as i32,
        });
        client
            .report_running_frame_completion(request)
            .await
            .into_diagnostic()
            .and(Ok(()))
    }

    async fn send_host_report(
        &self,
        render_host: pb::RenderHost,
        running_frames: Vec<pb::RunningFrameInfo>,
        core_detail: pb::CoreDetail,
    ) -> Result<()> {
        let mut client = self.client.lock().await;
        let mut request = pb::RqdReportStatusRequest::default();
        request.host_report = Some(pb::HostReport {
            host: Some(render_host),
            frames: running_frames,
            core_info: Some(core_detail),
        });
        client
            .report_status(request)
            .await
            .into_diagnostic()
            .and(Ok(()))
    }
}
