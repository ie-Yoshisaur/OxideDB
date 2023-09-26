pub struct CreateIndexData {
    idxname: String,
    tblname: String,
    fldname: String,
}

impl CreateIndexData {
    pub fn new(idxname: String, tblname: String, fldname: String) -> Self {
        Self {
            idxname,
            tblname,
            fldname,
        }
    }

    pub fn index_name(&self) -> &String {
        &self.idxname
    }

    pub fn table_name(&self) -> &String {
        &self.tblname
    }

    pub fn field_name(&self) -> &String {
        &self.fldname
    }
}
