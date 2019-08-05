use std::io;
use std::net::ToSocketAddrs;
use tokio_core::{
    net::TcpStream,
    reactor::Handle,
};

use lapin::{
    client::{ConnectionOptions,Client},
    queue::Queue,
    channel::{Channel,QueueDeclareOptions, BasicQosOptions, ExchangeDeclareOptions, BasicConsumeOptions},
    types::{FieldTable, AMQPValue},
};

use futures::{
    stream::Stream,
    future::{self, Future, Either},
    IntoFuture,
    sync::oneshot::{self,Canceled},
};

use std::rc::Rc;
use std::sync::Arc;

pub struct AmqpMessage {
    queue: Arc<String>,
    data: Vec<u8>,
    result: oneshot::Sender<Acknowledgement>,
}
impl AmqpMessage {
    fn new(queue: Arc<String>, data: Vec<u8>) -> (AmqpMessage,impl Future<Item = Acknowledgement, Error = AmqpError>) {
        let (tx,rx) = oneshot::channel();
        (AmqpMessage {
            queue: queue,
            data: data,
            result: tx,
        }, rx.map_err(|_:Canceled| AmqpError::HandlerCanceled))
    }
    pub fn queue(&self) -> &str {
        &self.queue as &str
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }
    pub fn ack(self) {
        self.result.send(Acknowledgement::Ack).ok();
    }
    pub fn reject(self) {
        self.result.send(Acknowledgement::Reject).ok();
    }
}

#[derive(Debug)]
pub enum AmqpError {
    HandlerCanceled,
    NoAddrResolveVariant,
    AddrResolve { addr: String, error: io::Error, },
    Heartbeat(io::Error),
    AmqpConnect(io::Error),
    CreateChannel(io::Error),
    TcpConnect { addr: std::net::SocketAddr, error: io::Error, },
    Qos(io::Error),
    QueueDeclare { queue: String, error: io::Error, },
    ExchangeDeclare { exchange: String, error: io::Error, },
    QueueBind {
        queue: String,
        exchange: String,
        error: io::Error,
    },
    QueueConsume { queue: String, error: io::Error, },
    RecvMessage { queue: String, error: io::Error, },
    AckMessage { queue: String, error: io::Error, },
    RejMessage { queue: String, error: io::Error, },
    Publish { target: String, error: io::Error, },
}

enum Acknowledgement {
    Ack,
    Reject,
}

#[derive(Debug,Clone)]
pub struct AmqpHost {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub pass: String,
}
impl AmqpHost {
    pub fn create_consumers<F>(self, handle: Handle, consumers: Vec<ConsumerConfig>, handler: &'static F) -> impl Future<Item = (), Error = AmqpError> 
        where F: Fn(AmqpMessage)
    {
        self.create(handle)
            .and_then(move |(client,heartbeat)| {
                future::join_all(consumers
                                 .into_iter()
                                 .map(move |con| con.create(&client,handler)))
                    .map(|_: Vec<()>| ())
                    .select2(heartbeat)
                    .map(|_| ())
                    .map_err(|ee| match ee {
                        Either::A((e,_)) |
                        Either::B((e,_)) => e,
                    })
            })
    }
    /*pub fn create_publisher(self) -> {
        
    }*/
    fn create(self, handle: Handle) -> impl Future<Item = (Client<TcpStream>, impl Future<Item=(), Error=AmqpError>) , Error = AmqpError> {
        let host = self.host.clone();
        (&self.host as &str, self.port)
            .to_socket_addrs()
            .map_err(|e| {
                AmqpError::AddrResolve {
                    addr: host,
                    error: e,
                }
            })
            .and_then(|mut v| v.next().ok_or(AmqpError::NoAddrResolveVariant))
            .into_future()
            .and_then(move |amqp_addr| {
                info!("RabbitMQ expected @ {}", amqp_addr);
                TcpStream::connect(&amqp_addr, &handle)
                    .map_err({
                        let amqp_addr = amqp_addr.clone();
                        move |e| {
                            AmqpError::TcpConnect {
                                addr: amqp_addr,
                                error: e,
                            }
                        }
                    })
                    .map(move |stream| (stream,amqp_addr))
            })
            .and_then(move |(stream,amqp_addr)| {
                info!("connected to {}", amqp_addr);
                let options = ConnectionOptions {
                    username: self.user,
                    password: self.pass,
                    heartbeat: 4,
                    ..Default::default()
                };
                info!("authorizing as {:?}", options);
                Client::connect(stream, options)
                    .map_err(AmqpError::AmqpConnect)
            })
            .and_then(move |(client, heartbeat)| {
                info!("authorize success");
                future::ok((client, heartbeat.map_err(AmqpError::Heartbeat)))
            })
    }
}

#[derive(Debug,Clone)]
pub struct ConsumerConfig {
    queue: QueueConfig,
    bindings: Vec<Binding>,
    consumer_tag: String,
    options: BasicConsumeOptions, 
    args: FieldTable,
}
impl ConsumerConfig {
    pub fn new<S: ToString>(queue: QueueConfig, consumer_tag: S) -> ConsumerConfig {
        ConsumerConfig {
            queue: queue,
            bindings: Vec::new(),
            consumer_tag: consumer_tag.to_string(),
            options: BasicConsumeOptions::default(), 
            args: FieldTable::new(),
        }
    }
    pub fn add_binding(mut self, binding: Binding) -> ConsumerConfig {
        self.bindings.push(binding);
        self
    }
    pub fn create<F>(self, rabbitmq_client: &Client<TcpStream>, handler: &'static F) -> impl Future<Item=(), Error = AmqpError>
        where F: Fn(AmqpMessage)
    {
        let con_q = self.queue;
        let con_binds = self.bindings;
        let con_tag = self.consumer_tag;
        let con_opt = self.options;
        let con_args = self.args;
        rabbitmq_client
            .create_channel()
            .map_err(AmqpError::CreateChannel)
            .and_then({
                let con_tag = con_tag.clone();
                move |channel| {
                    info!("channel {} created for consumer: {} [{}]", channel.id, con_q.name, con_tag);
                    let channel = Rc::new(channel);
                    match con_binds.len() {
                        0 => Either::A(con_q.create(channel.clone())),
                        _ => Either::B(con_q.create_binded(channel.clone(),con_binds)),
                    }.map(move |queue| (channel,queue))
                }
            })
            .and_then(move |(channel,queue)| {
                let q = Arc::new(queue.name());
                info!("start consuming: {}",q);                        
                channel.basic_consume(&queue, &con_tag, con_opt, con_args)
                    .map_err({
                        let q = q.clone();
                        move |e| AmqpError::QueueConsume {
                            queue: q.to_string(),
                            error: e,
                        }
                    })
                    .map({
                        let q = q.clone();
                        move |stream| stream.map_err(move |e| AmqpError::QueueConsume {
                            queue: q.to_string(),
                            error: e,
                        })
                    })
                    .flatten_stream()
                    .and_then({
                        let q = q.clone();
                        move |msg| {
                            let (am,res) = AmqpMessage::new(q.clone(),msg.data);
                            let dtag = msg.delivery_tag;
                            handler(am);
                            res.map(move |r| (dtag,r))
                        }
                    })
                    .for_each(move |(delivery_tag,result)| {
                        match result {
                            Acknowledgement::Ack => Either::A(channel.basic_ack(delivery_tag,false)
                                                              .map_err({
                                                                  let q = q.clone();
                                                                  move |e| AmqpError::AckMessage { queue: q.to_string(), error: e }
                                                              })),
                            Acknowledgement::Reject => Either::B(channel.basic_reject(delivery_tag,false)
                                                             .map_err({
                                                                 let q = q.clone();
                                                                 move |e| AmqpError::RejMessage { queue: q.to_string(), error: e }
                                                             })),
                        }
                    })
            })       
    }
}

#[derive(Clone,Copy,Debug)]
pub enum CompatMode {
    None,
    Compat357,
}

#[derive(Debug,Clone)]
pub struct QueueConfig {
    name: String,
    options: QueueDeclareOptions,
    args: FieldTable,
    prefetch_count: Option<u16>,

    compatibility: CompatMode
}
impl QueueConfig {
    pub fn new<S: ToString>(name: S) -> QueueConfig {
        QueueConfig {
            name: name.to_string(),
            options: Default::default(),
            args: FieldTable::default(),
            prefetch_count: None,

            compatibility: CompatMode::None,
        }
    }
    pub fn with_compatibility<S: ToString>(name: S, compat: CompatMode) -> QueueConfig {
        QueueConfig {
            name: name.to_string(),
            options: Default::default(),
            args: FieldTable::default(),
            prefetch_count: None,

            compatibility: compat,
        }
    }
    pub fn set_durable(mut self) -> QueueConfig {
        self.options.durable = true;
        self
    }
    pub fn set_auto_delete(mut self) -> QueueConfig {
        self.options.auto_delete = true;
        self
    }
    pub fn set_exclusive(mut self) -> QueueConfig {
        self.options.exclusive = true;
        self
    }
    pub fn set_prefetch_count(mut self, prefetch: u16) -> QueueConfig {
        self.prefetch_count = Some(prefetch);
        self
    }
    pub fn set_max_length(mut self, n: u32) -> QueueConfig {
        match self.compatibility {
            CompatMode::None => self.args.insert("x-max-length".to_string(), AMQPValue::LongUInt(n)),
            CompatMode::Compat357 => self.args.insert("x-max-length".to_string(), AMQPValue::LongInt(n as i32)),
        };
        self
    }

    pub fn create(self, shared_channel: Rc<Channel<TcpStream>>) -> impl Future<Item = Queue, Error = AmqpError> {
        let qname = self.name.clone();
        let prefetch_count = self.prefetch_count;
        shared_channel.queue_declare(&self.name,self.options,self.args)
            .map_err(move |e| AmqpError::QueueDeclare {
                queue: qname,
                error: e,
            })
            .map(move |queue| (queue,prefetch_count))
            .and_then({            
                let channel = shared_channel.clone();
                move |(queue,prefetch_count)| {
                    match prefetch_count {
                        Some(prefetch) => Either::A({
                            channel.basic_qos(BasicQosOptions {
                                prefetch_count: prefetch,
                                ..Default::default()
                            })
                                .map_err(AmqpError::Qos)
                                .map(|()| queue)
                        }),
                        None => Either::B(future::ok(queue))
                    }
                }
            })
    }

    pub fn create_binded(self, shared_channel: Rc<Channel<TcpStream>>, bindings: Vec<Binding>) -> impl Future<Item = Queue, Error = AmqpError> {
                future::join_all(bindings
                                  .into_iter()
                                  .map({
                                      let channel = shared_channel.clone();
                                      move |Binding { exchange: exchange_config, routing_key }| {
                                          let ename = exchange_config.name.clone();
                                          channel
                                              .exchange_declare(&exchange_config.name, exchange_config.exchange_type.as_str(), exchange_config.options, exchange_config.args)
                                              .map_err({
                                                  let ename = ename.clone();
                                                  move |e| AmqpError::ExchangeDeclare {
                                                      exchange: ename,
                                                      error: e,
                                                  }
                                              })
                                              .map(move |()| (ename,routing_key))
                                      }
                                  }))
                    .map(move |binding_vec| (binding_vec,shared_channel))
                    .and_then(move |(binding_vec,channel)| {
                        let qname = self.name.clone();
                        let prefetch_count = self.prefetch_count;
                        channel.queue_declare(&self.name,self.options,self.args)
                            .map_err(move |e| AmqpError::QueueDeclare {
                                queue: qname,
                                error: e,
                            })
                            .map(move |queue| (queue,prefetch_count))
                            .and_then({            
                                let channel = channel.clone();
                                move |(queue,prefetch_count)| {
                                    match prefetch_count {
                                        Some(prefetch) => Either::A({
                                            channel.basic_qos(BasicQosOptions {
                                                prefetch_count: prefetch,
                                                ..Default::default()
                                            })
                                                .map_err(AmqpError::Qos)
                                                .map(|()| (queue,binding_vec,channel))
                                        }),
                                        None => Either::B(future::ok((queue,binding_vec,channel)))
                                    }
                                }
                            })
                    })
                    .and_then(move |(queue,binding_vec,channel)| {
                        let qname = queue.name();
                        future::join_all(binding_vec
                                          .into_iter()
                                          .map(move |(ex_name,routing)| {
                                              info!("binding queue {} to exchange {}", qname, ex_name);
                                              let tmp_name = qname.clone();
                                              channel
                                                  .queue_bind(&qname,
                                                              &ex_name,
                                                              &routing,
                                                              Default::default(),
                                                              FieldTable::new())
                                                  .map_err(move |e| AmqpError::QueueBind {
                                                      queue: tmp_name,
                                                      exchange: ex_name,
                                                      error: e,
                                                  })
                                          }))
                            .map(|_| queue)
                    })           
    }
}

#[derive(Debug,Clone)]
pub struct Binding {
    exchange: ExchangeConfig,
    routing_key: String,
}
impl Binding {
    pub fn new(exchange: ExchangeConfig, routing: String) -> Binding {
        Binding {
            exchange: exchange,
            routing_key: routing,
        }
    }
}

#[derive(Debug,Clone,Copy)]
pub enum ExchangeType {
    Fanout,
    Direct,
    Topic,
}
impl ExchangeType {
    fn as_str(&self) -> &str {
        match self {
            ExchangeType::Fanout => "fanout",
            ExchangeType::Direct => "direct",
            ExchangeType::Topic => "topic",
        }
    }
}

#[derive(Debug,Clone)]
pub struct ExchangeConfig {
    name: String,
    exchange_type: ExchangeType,
    options: ExchangeDeclareOptions,
    args: FieldTable,
}
impl ExchangeConfig {
    pub fn new<S: ToString>(name: S, exchange_type: ExchangeType) -> ExchangeConfig {
        ExchangeConfig {
            name: name.to_string(),
            exchange_type: exchange_type,
            options: ExchangeDeclareOptions::default(),
            args: FieldTable::new(),
        }
    }
    pub fn set_durable(mut self) -> ExchangeConfig {
        self.options.durable = true;
        self
    }
}


