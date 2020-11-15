extern crate chrono;

use async_trait::async_trait;

use super::Sink;

#[derive(Deserialize, Clone)]
pub struct CollectdExecConfig {
    pub host_plugin: String,
    pub type_co2: String,
    pub type_temperature: String,
}

pub struct CollectdExecSink {
    points: Vec<super::Measurement>,
    config: CollectdExecConfig,
}

#[async_trait]
impl Sink for CollectdExecSink {
    async fn add_measurement(&mut self, measurement: &super::Measurement) {
        self.points.push(measurement.to_owned());
    }

    async fn submit(&mut self) {
        let config = &self.config;
        self.points.retain(|point| {
            let mut fields: Vec<_> = point.fields.iter().collect();
            fields.sort_by(|a, b| a.0.cmp(b.0));
            for (field_name, field_value) in fields.iter() {
                let type_name = match field_name.as_str() {
                    "co2" => &config.type_co2,
                    "temperature" => &config.type_temperature,
                    _ => "gauge",
                };
                let identifier = format!("{}/{}", config.host_plugin, type_name);
                println!(
                    "PUTVAL {} {}:{}",
                    identifier,
                    point.timestamp.timestamp(),
                    field_value
                );
            }
            false
        });
    }
}

impl CollectdExecSink {
    pub fn from_config(config: &CollectdExecConfig) -> Box<dyn Sink + Send> {
        Box::new(CollectdExecSink {
            points: Vec::new(),
            config: config.clone(),
        })
    }
}
