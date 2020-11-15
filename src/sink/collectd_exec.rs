extern crate chrono;

use async_trait::async_trait;

use super::Sink;

#[derive(Deserialize)]
pub struct CollectdExecConfig {}

pub struct CollectdExecSink {
    points: Vec<super::Measurement>,
}

#[async_trait]
impl Sink for CollectdExecSink {
    async fn add_measurement(&mut self, measurement: &super::Measurement) {
        self.points.push(measurement.to_owned());
    }

    async fn submit(&mut self) {
        self.points.retain(|point| {
            let mut fields: Vec<_> = point.fields.iter().collect();
            fields.sort_by(|a, b| a.0.cmp(b.0));
            for (field_name, field_value) in fields.iter() {
                let identifier = format!("testhost/testdevice/{}", field_name);
                println!("PUTVAL {} {}:{}", identifier, point.timestamp.timestamp(), field_value);
            }
            false
        });
    }
}

impl CollectdExecSink {
    pub fn from_config(_config: &CollectdExecConfig) -> Box<dyn Sink + Send> {
        Box::new(CollectdExecSink { points: Vec::new() })
    }
}
