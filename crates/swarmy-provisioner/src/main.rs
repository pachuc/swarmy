fn main() -> anyhow::Result<()> {
    swarmy_provisioner::role::run(std::env::args_os().skip(1).collect())
}
