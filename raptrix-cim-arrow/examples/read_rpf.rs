use std::env;
use raptrix_cim_arrow::read_rpf_tables;

fn main() {
    let path = env::args().nth(1).expect("usage: read_rpf <path>");
    match read_rpf_tables(&path) {
        Ok(tables) => {
            println!("OK: {} tables", tables.len());
            for (name, batch) in &tables {
                println!("  {name}: rows={} cols={}", batch.num_rows(), batch.num_columns());
            }
        }
        Err(e) => {
            println!("ERROR: {e:#}");
            std::process::exit(1);
        }
    }
}
