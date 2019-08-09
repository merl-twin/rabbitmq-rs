#[macro_use]
extern crate log;
extern crate futures;
extern crate tokio_core;
extern crate lapin_futures as lapin;

mod rabbit;

pub use rabbit::{
    AmqpMessage,
    AmqpHost,
    QueueConfig,
    ExchangeConfig,
    ConsumerConfig,
    Binding,
    AmqpError,
    ExchangeType,
    CompatMode,
    Publisher,
    FuturePublisher,
};


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {

    }
}
