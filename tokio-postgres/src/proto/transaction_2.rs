use futures::{Async, Future, Poll};
use proto::simple_query::SimpleQueryFuture;
use raii_counter::{Counter, WeakCounter};
use state_machine_future::RentToOwn;
use std::ops::{Deref, DerefMut};
use Client;

use Error;

pub struct CountedClient {
    client: Client,
    _counter: Counter,
}

impl CountedClient {
    pub fn new(client: Client) -> (WeakCounter, Self) {
        let weak = WeakCounter::new();
        let client = Self {
            client,
            _counter: weak.spawn_upgrade(),
        };

        (weak, client)
    }
}

impl Deref for CountedClient {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl DerefMut for CountedClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

#[derive(StateMachineFuture)]
pub enum Transaction2<FC, F, T, E>
where
    F: Future<Item = T, Error = E>,
    FC: Fn(CountedClient) -> F,
    E: From<Error>,
{
    #[state_machine_future(start, transitions(Beginning))]
    Start { client: Client, future_closure: FC },
    #[state_machine_future(transitions(Running))]
    Beginning {
        client: Client,
        begin: SimpleQueryFuture,
        future_closure: FC,
    },
    #[state_machine_future(transitions(Finishing))]
    Running {
        client: Client,
        future: F,
        counter: WeakCounter,
    },
    #[state_machine_future(transitions(Finished))]
    Finishing {
        future: SimpleQueryFuture,
        result: Result<T, E>,
        counter: WeakCounter,
    },
    #[state_machine_future(ready)]
    Finished(T),
    #[state_machine_future(error)]
    Failed(E),
}

impl<FC, F, T, E> PollTransaction2<FC, F, T, E> for Transaction2<FC, F, T, E>
where
    F: Future<Item = T, Error = E>,
    FC: Fn(CountedClient) -> F,
    E: From<Error>,
{
    fn poll_start<'a>(
        state: &'a mut RentToOwn<'a, Start<FC, F, T, E>>,
    ) -> Poll<AfterStart<FC, F, T, E>, E> {
        let state = state.take();
        transition!(Beginning {
            begin: state.client.0.batch_execute("BEGIN"),
            client: state.client,
            future_closure: state.future_closure,
        })
    }

    fn poll_beginning<'a>(
        state: &'a mut RentToOwn<'a, Beginning<FC, F, T, E>>,
    ) -> Poll<AfterBeginning<F, T, E>, E> {
        try_ready!(state.begin.poll());
        let state = state.take();
        let (counter, counted_client) = CountedClient::new(Client(state.client.0.clone()));
        transition!(Running {
            future: (state.future_closure)(counted_client),
            client: state.client,
            counter,
        })
    }

    fn poll_running<'a>(
        state: &'a mut RentToOwn<'a, Running<F, T, E>>,
    ) -> Poll<AfterRunning<T, E>, E> {
        let result = match state.future.poll() {
            Ok(Async::NotReady) => return Ok(Async::NotReady),
            Ok(Async::Ready(t)) => Ok(t),
            Err(err) => Err(err),
        };

        let state = state.take();
        match result {
            Ok(t) => transition!(Finishing {
                future: state.client.0.batch_execute("COMMIT"),
                result: Ok(t),
                counter: state.counter
            }),
            Err(err) => transition!(Finishing {
                future: state.client.0.batch_execute("ROLLBACK"),
                result: Err(err),
                counter: state.counter,
            }),
        }
    }

    fn poll_finishing<'a>(
        state: &'a mut RentToOwn<'a, Finishing<T, E>>,
    ) -> Poll<AfterFinishing<T>, E> {
        match state.future.poll() {
            Ok(Async::NotReady) => return Ok(Async::NotReady),
            Ok(Async::Ready(())) => {
                let state = state.take();
                assert!(
                    state.counter.count() == 0,
                    "a reference to the postgres connection was left dangling after transaction completion"
                );

                match state.result {
                    Ok(res) => transition!(Finished(res)),
                    Err(err) => Err(err.into()),
                }
            }
            Err(e) => {
                let state = state.take();
                assert!(
                    state.counter.count() == 0,
                    "a reference to the postgres connection was left dangling after transaction completion"
                );
                match state.result {
                    Ok(_) => Err(e.into()),
                    // prioritize the future's error over the rollback error
                    Err(e) => Err(e),
                }
            }
        }
    }
}

impl<FC, F, T, E> Transaction2Future<FC, F, T, E>
where
    F: Future<Item = T, Error = E>,
    FC: Fn(CountedClient) -> F,
    E: From<Error>,
{
    pub fn new(client: Client, future_closure: FC) -> Transaction2Future<FC, F, T, E> {
        Transaction2::start(client, future_closure)
    }
}

// struct Append<V, F> {
//     value: V,
//     future: F,
// }

// impl<V, F> Future for Append<V, F>
// where
//     F: Future,
// {
//     type Item = (V, F::Item);
//     type Error = F::Error;

//     fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
//         let future_val = try_ready!(self.future.poll());
//         Ok(Async::Ready((self.value, future_val))
//     }
// }
