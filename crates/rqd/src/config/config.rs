use crate::config::error::RqdConfigError;
use bytesize::ByteSize;
use config::{Config as ConfigBase, Environment, File};
use serde::Deserialize;
use std::{env, sync::Arc};

static DEFAULT_CONFIG_FILE: &str = "~/.local/share/rqd.yaml";

//===Config Types===

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct LoggingConfig {
    // Logging level: debug|info|warning|error
    pub level: String,
    // Path to the log file if `file_appender` is enabled
    pub path: String,
    // Log to stdout if file_appender is False
    pub file_appender: bool,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "debug".to_string(),
            path: "/opt/rqd/logs/scheduler.log".to_string(),
            file_appender: false,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct GrpcConfig {
    pub rqd_port: u16,
    pub cuebot_url: String,
}

impl Default for GrpcConfig {
    fn default() -> GrpcConfig {
        GrpcConfig {
            rqd_port: 4778,
            cuebot_url: "localhost:4343".to_string(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct MachineConfig {
    pub monitor_interval_seconds: u64,
    pub use_ip_as_hostname: bool,
    pub override_real_values: Option<OverrideConfig>,
    pub custom_tags: Vec<String>,
    pub nimby_mode: bool,
    pub facility: String,
    pub cpuinfo_path: String,
    pub distro_release_path: String,
    pub proc_stat_path: String,
    pub inittab_path: String,
    pub proc_loadavg_path: String,
    pub temp_path: String,
    pub core_multiplier: u32,
}

impl Default for MachineConfig {
    fn default() -> MachineConfig {
        MachineConfig {
            monitor_interval_seconds: 3,
            use_ip_as_hostname: false,
            override_real_values: None,
            custom_tags: vec![],
            nimby_mode: false,
            facility: "cloud".to_string(),
            cpuinfo_path: "/etc/cpuinfo".to_string(),
            distro_release_path: "/etc/*-release".to_string(),
            proc_stat_path: "/proc/stat".to_string(),
            inittab_path: "/etc/inittab".to_string(),
            proc_loadavg_path: "/proc/loadavg".to_string(),
            temp_path: "/tmp".to_string(),
            core_multiplier: 100,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct OverrideConfig {
    pub cores: Option<u64>,
    pub procs: Option<u64>,
    pub memory_size: Option<ByteSize>,
    pub workstation_mode: Option<bool>,
    pub hostname: Option<String>,
    pub os: Option<String>,
}

impl Default for OverrideConfig {
    fn default() -> OverrideConfig {
        OverrideConfig {
            cores: None,
            procs: None,
            memory_size: None,
            workstation_mode: None,
            hostname: None,
            os: None,
        }
    }
}

//===Config Traits===

/// Defines a type that can be constructed using the configuration values
pub trait FromConfig: Sized {
    fn from_config(config: &Config) -> Result<Self, RqdConfigError>;
}

/// Defines a type that can be constructed using the configuration values
pub trait FromConfigArc: Sized {
    fn from_config(config: &Config) -> Result<Arc<Self>, RqdConfigError>;
}

//===Config Loader===

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(default)]
pub struct Config {
    pub logging: LoggingConfig,
    pub grpc: GrpcConfig,
    pub machine: MachineConfig,
}

impl Config {
    // load the current config from the system config and environment variables
    pub fn load() -> Result<Self, RqdConfigError> {
        let mut required = false;
        let config_file = match env::var("OPENCUE_RQD_CONFIG") {
            Ok(v) => {
                required = true;
                v
            }
            Err(_) => DEFAULT_CONFIG_FILE.to_string(),
        };

        println!(" INFO Config::load: using config file: {:?}", config_file);

        let config = ConfigBase::builder()
            .add_source(File::with_name(&*config_file).required(required))
            .add_source(Environment::with_prefix("OPENRQD").separator("_"))
            .build()
            .map_err(|err| {
                RqdConfigError::LoadConfigError(format!(
                    "{:?} config could not be loaded",
                    &config_file
                ))
            })?;

        Config::deserialize(config).map_err(|err| {
            RqdConfigError::LoadConfigError(format!(
                "{:?} config could not be deserialized",
                &config_file
            ))
        })
    }

    pub fn load_file_and_env<P: AsRef<str>>(path: P) -> Result<Self, RqdConfigError> {
        let config = ConfigBase::builder()
            .add_source(File::with_name(path.as_ref()))
            .add_source(Environment::with_prefix("VNPM").separator("_"))
            .build();

        config
            .map(|c| Config::deserialize(c).unwrap())
            .map_err(|err| {
                RqdConfigError::LoadConfigError(format!(
                    "{:?} config could not be loaded",
                    path.as_ref()
                ))
            })
    }
    pub fn load_file<P: AsRef<str>>(path: P) -> Result<Self, RqdConfigError> {
        let config = ConfigBase::builder()
            .add_source(File::with_name(path.as_ref()))
            .build();

        config
            .map(|c| Config::deserialize(c).unwrap())
            .map_err(|err| {
                RqdConfigError::LoadConfigError(format!(
                    "{:?} config could not be loaded",
                    path.as_ref()
                ))
            })
    }
}
