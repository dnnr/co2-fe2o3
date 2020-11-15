extern crate chrono;

use async_trait::async_trait;

use super::Sink;

#[derive(Deserialize)]
pub struct CollectdExecConfig {
    pub identifier: String

}

pub struct CollectdExecSink {
    points: Vec<super::Measurement>,
    identifier: String,
}

#[async_trait]
impl Sink for CollectdExecSink {
    async fn add_measurement(&mut self, measurement: &super::Measurement) {
        self.points.push(measurement.to_owned());
    }

    async fn submit(&mut self) {
        let identifier = &self.identifier;
        self.points.retain(|point| {
            let mut fields: Vec<_> = point.fields.iter().collect();
            fields.sort_by(|a, b| a.0.cmp(b.0));
            for (field_name, field_value) in fields.iter() {
                let identifier = format!("{}/{}", identifier, field_name);
                println!("PUTVAL {} {}:{}", identifier, point.timestamp.timestamp(), field_value);
            }
            false
        });
    }
}

impl CollectdExecSink {
    pub fn from_config(config: &CollectdExecConfig) -> Box<dyn Sink + Send> {
        // Sanitize identifier by removing trailing slashes
        let identifier = config.identifier.trim_end_matches("/");

        Box::new(CollectdExecSink {
            points: Vec::new(),
            identifier: String::from(identifier),
        })
    }
}
