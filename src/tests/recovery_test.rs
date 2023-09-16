use crate::file::block_id::BlockId;
use crate::file::page::Page;
use crate::server::oxide_db::OxideDB;
use std::fs::remove_dir_all;
use std::mem::size_of;
use std::path::PathBuf;
use std::sync::Arc;

const I32_SIZE: usize = size_of::<i32>();

#[test]
fn recovery_test() {
    let test_directory = PathBuf::from("recoverytest");
    {
        let db = Arc::new(OxideDB::new_for_debug(test_directory.clone(), 400, 8));
        let block0 = BlockId::new("testfile".to_string(), 0);
        let block1 = BlockId::new("testfile".to_string(), 1);

        initialize(db.clone(), block0.clone(), block1.clone());
        modify(db.clone(), block0.clone(), block1.clone());
    }
    {
        let db = Arc::new(OxideDB::new_for_debug(test_directory.clone(), 400, 8));
        let block0 = BlockId::new("testfile".to_string(), 0);
        let block1 = BlockId::new("testfile".to_string(), 1);

        recover(db.clone(), block0.clone(), block1.clone());
    }
    remove_dir_all(test_directory).unwrap();
}

fn initialize(db: Arc<OxideDB>, block0: BlockId, block1: BlockId) {
    let mut transaction0 = db.new_transaction();
    let mut transaction1 = db.new_transaction();
    transaction0.pin(block0.clone());
    transaction1.pin(block1.clone());

    let mut position = 0;
    for _ in 0..6 {
        transaction0
            .set_int(block0.clone(), position, position, false)
            .unwrap();
        transaction1
            .set_int(block1.clone(), position, position, false)
            .unwrap();
        position += I32_SIZE as i32;
    }

    transaction0
        .set_string(block0.clone(), 30, &"abc".to_string(), false)
        .unwrap();
    transaction1
        .set_string(block1.clone(), 30, &"def".to_string(), false)
        .unwrap();
    transaction0.commit();
    transaction1.commit();

    let (init_vec0, init_vec1, init_str0, init_str1) =
        print_values("After Initialization:", &db, block0.clone(), block1.clone());
    assert_eq!(init_vec0, vec![0, 4, 8, 12, 16, 20]);
    assert_eq!(init_vec1, vec![0, 4, 8, 12, 16, 20]);
    assert_eq!(init_str0, "abc");
    assert_eq!(init_str1, "def");
}

fn modify(db: Arc<OxideDB>, block0: BlockId, block1: BlockId) {
    let mut transaction2 = db.new_transaction();
    let mut transaction3 = db.new_transaction();
    transaction2.pin(block0.clone());
    transaction3.pin(block1.clone());

    let mut position = 0;
    for _ in 0..6 {
        transaction2
            .set_int(block0.clone(), position, position + 100, true)
            .unwrap();
        transaction3
            .set_int(block1.clone(), position, position + 100, true)
            .unwrap();
        position += I32_SIZE as i32;
    }

    transaction2
        .set_string(block0.clone(), 30, &"uvw".to_string(), true)
        .unwrap();
    transaction3
        .set_string(block1.clone(), 30, &"xyz".to_string(), true)
        .unwrap();

    {
        let locked_buffer_manager = db.get_buffer_manager().lock().unwrap();
        locked_buffer_manager.flush_all(2).unwrap();
        locked_buffer_manager.flush_all(3).unwrap();
    }

    let (mod_vec0, mod_vec1, mod_str0, mod_str1) =
        print_values("After modification:", &db, block0.clone(), block1.clone());
    assert_eq!(mod_vec0, vec![100, 104, 108, 112, 116, 120]);
    assert_eq!(mod_vec1, vec![100, 104, 108, 112, 116, 120]);
    assert_eq!(mod_str0, "uvw");
    assert_eq!(mod_str1, "xyz");

    transaction2.rollback();

    let (rollback_vec0, rollback_vec1, rollback_str0, rollback_str1) =
        print_values("After rollback:", &db, block0.clone(), block1.clone());
    assert_eq!(rollback_vec0, vec![0, 4, 8, 12, 16, 20]);
    assert_eq!(rollback_vec1, vec![100, 104, 108, 112, 116, 120]);
    assert_eq!(rollback_str0, "abc");
    assert_eq!(rollback_str1, "xyz");
}

fn recover(db: Arc<OxideDB>, block0: BlockId, block1: BlockId) {
    let mut transaction = db.new_transaction();
    transaction.recover();

    let (recover_vec0, recover_vec1, recover_str0, recover_str1) =
        print_values("After recovery:", &db, block0.clone(), block1.clone());
    assert_eq!(recover_vec0, vec![0, 4, 8, 12, 16, 20]);
    assert_eq!(recover_vec1, vec![0, 4, 8, 12, 16, 20]);
    assert_eq!(recover_str0, "abc");
    assert_eq!(recover_str1, "def");
}

fn print_values(
    msg: &str,
    db: &Arc<OxideDB>,
    block0: BlockId,
    block1: BlockId,
) -> (Vec<i32>, Vec<i32>, String, String) {
    println!("{}", msg);

    let block_size = db.get_file_manager().lock().unwrap().get_block_size();
    let mut page0 = Page::new_from_blocksize(block_size);
    let mut page1 = Page::new_from_blocksize(block_size);
    let mut vec0 = Vec::new();
    let mut vec1 = Vec::new();

    {
        let locked_file_manager = db.get_file_manager().lock().unwrap();
        locked_file_manager
            .read(&block0.clone(), &mut page0)
            .unwrap();
        locked_file_manager
            .read(&block1.clone(), &mut page1)
            .unwrap();
    }

    let mut position = 0;
    for _ in 0..6 {
        vec0.push(page0.get_int(position).unwrap());
        vec1.push(page1.get_int(position).unwrap());
        position += I32_SIZE;
    }

    let str0 = page0.get_string(30).unwrap();
    let str1 = page1.get_string(30).unwrap();

    (vec0, vec1, str0, str1)
}
