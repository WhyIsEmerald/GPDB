mod db_bench_impl;

fn main() {
    if let Err(e) = db_bench_impl::run() {
        eprintln!("Benchmark failed: {:?}", e);
        std::process::exit(1);
    }
}
