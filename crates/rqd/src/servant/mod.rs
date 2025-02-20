/// Implement Rqd modules for rqd.proto's interfaces
use crate::{config::config::Config, monitor::system::Machine, running_frame::RunningFrameCache};

use opencue_proto::rqd::{
    rqd_interface_server::RqdInterfaceServer, running_frame_server::RunningFrameServer,
};
use rqd_servant::{MachineImpl, RqdServant};
use running_frame_servant::RunningFrameServant;
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};
use tonic::transport::Server;

pub mod rqd_servant;
pub mod running_frame_servant;

pub type Result<T> = core::result::Result<T, tonic::Status>;

pub async fn serve(
    config: Config,
    running_frame_cache: Arc<RunningFrameCache>,
    machine: Arc<MachineImpl>,
) -> Result<()> {
    let address: SocketAddr =
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), config.grpc.rqd_port);

    let running_frame_servant = RunningFrameServant::init(Arc::clone(&running_frame_cache));
    let rqd_servant = RqdServant::init(config, Arc::clone(&running_frame_cache), machine);

    Server::builder()
        .add_service(RunningFrameServer::new(running_frame_servant))
        .add_service(RqdInterfaceServer::new(rqd_servant))
        .serve(address)
        .await
        .map_err(|err| tonic::Status::from_error(Box::new(err)))
}
