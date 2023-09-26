use crate::parse::query_data::QueryData;

pub struct CreateViewData {
    viewname: String,
    qrydata: QueryData,
}

impl CreateViewData {
    pub fn new(viewname: String, qrydata: QueryData) -> Self {
        Self { viewname, qrydata }
    }

    pub fn view_name(&self) -> &String {
        &self.viewname
    }

    pub fn view_def(&self) -> String {
        self.qrydata.to_string()
    }
}
