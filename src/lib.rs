#![allow(incomplete_features)]
#![feature(generic_const_exprs)]

#[cfg(not(feature = "sub60-cache"))]
mod any_cache;
mod async_read_ext;
mod context;
mod event_loop;
mod nix_store;
#[cfg(not(feature = "sub60-cache"))]
mod noop_cache;
mod protocol;
mod push;
mod real_cache;
mod real_context;
mod real_stop_progress_reporter;
mod real_token_fetcher;
mod run;
mod stop;
mod token;
mod tokio;

#[derive(Debug, clap::Subcommand)]
#[expect(clippy::large_enum_variant)]
pub enum Command {
    Run(run::RunArgs),
    Push(push::PushArgs),
    Stop(stop::StopArgs),
}

pub fn run(command: Command) {
    match command {
        Command::Run(args) => run::run(args),
        Command::Push(args) => push::push(args),
        Command::Stop(args) => stop::stop(args),
    }
}
