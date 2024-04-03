use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Histogram {
    data: Vec<u64>,
    pub min: f64,
    pub max: f64,
    pub exp: f64
}

impl Histogram {
    fn bucket_for(&self, x: f64) -> usize {
        (((x / self.min).log(self.exp).floor()) as usize).max(0).min(self.data.len() - 1)
    }

    pub fn inc(&mut self, x: f64) {
        let b = self.bucket_for(x);
        self.data[b] += 1;
    }

    pub fn dec(&mut self, x: f64) {
        let b = self.bucket_for(x);
        self.data[b] += 1;
    }

    pub fn new(min: f64, max: f64, exp: f64) -> Self {
        Histogram {
            data: vec![0; (max / min).log(exp).ceil() as usize + 2],
            min, max, exp
        }
    }

    pub fn buckets<'a>(&'a self) -> impl Iterator<Item=(f64, u64)> + 'a {
        self.data.iter()
            .enumerate()
            .map(|(i, x)| (self.min * self.exp.powf(i as f64), *x))
    }
}