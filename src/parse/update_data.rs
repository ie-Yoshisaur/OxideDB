use crate::parse::create_index_data::CreateIndexData;
use crate::parse::create_table_data::CreateTableData;
use crate::parse::create_view_data::CreateViewData;
use crate::parse::delete_data::DeleteData;
use crate::parse::insert_data::InsertData;
use crate::parse::modify_data::ModifyData;

pub enum UpdateData {
    Insert(InsertData),
    Delete(DeleteData),
    Modify(ModifyData),
    CreateTable(CreateTableData),
    CreateView(CreateViewData),
    CreateIndex(CreateIndexData),
}
