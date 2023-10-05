pub struct BufferNeeds;

impl BufferNeeds {
    pub fn best_root(available: i32, size: i32) -> i32 {
        let avail = available - 2; //reserve a couple
        if avail <= 1 {
            return 1;
        }
        let mut k = i32::MAX;
        let mut i = 1.0;
        while k > avail {
            i += 1.0;
            k = (size as f64).powf(1.0 / i).ceil() as i32;
        }
        k
    }

    pub fn best_factor(available: i32, size: i32) -> i32 {
        let avail = available - 2; //reserve a couple
        if avail <= 1 {
            return 1;
        }
        let mut k = size;
        let mut i = 1.0;
        while k > avail {
            i += 1.0;
            k = (size as f64 / i).ceil() as i32;
        }
        k
    }
}
