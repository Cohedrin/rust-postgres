use futures::{Async, Future, Poll};
use proto::simple_query::SimpleQueryFuture;
use state_machine_future::RentToOwn;
use Client;

use DerefMut2;
use Error;

#[derive(StateMachineFuture)]
pub enum Transaction2<FC, F, C, T, E>
where
    C: DerefMut2<Target = Client>,
    F: Future<Item = (C, T), Error = (C, E)>,
    FC: Fn(C) -> F,
    E: From<Error>,
{
    #[state_machine_future(start, transitions(Beginning))]
    Start { client: C, future_closure: FC },
    #[state_machine_future(transitions(Running))]
    Beginning {
        client: C,
        begin: SimpleQueryFuture,
        future_closure: FC,
    },
    #[state_machine_future(transitions(Finishing))]
    Running { future: F },
    #[state_machine_future(transitions(Finished))]
    Finishing {
        client: C,
        future: SimpleQueryFuture,
        result: Result<T, E>,
    },
    #[state_machine_future(ready)]
    Finished((C, T)),
    #[state_machine_future(error)]
    Failed((C, E)),
}

impl<FC, F, C, T, E> PollTransaction2<FC, F, C, T, E> for Transaction2<FC, F, C, T, E>
where
    C: DerefMut2<Target = Client>,
    F: Future<Item = (C, T), Error = (C, E)>,
    FC: Fn(C) -> F,
    E: From<Error>,
{
    fn poll_start<'a>(
        state: &'a mut RentToOwn<'a, Start<FC, F, C, T, E>>,
    ) -> Poll<AfterStart<FC, F, C, T, E>, (C, E)> {
        let mut state = state.take();
        transition!(Beginning {
            begin: state.client.deref_mut2().0.batch_execute("BEGIN"),
            client: state.client,
            future_closure: state.future_closure,
        })
    }

    fn poll_beginning<'a>(
        state: &'a mut RentToOwn<'a, Beginning<FC, F, C, T, E>>,
    ) -> Poll<AfterBeginning<F, C, T, E>, (C, E)> {
        match state.begin.poll() {
            Ok(Async::Ready(_)) => (),
            Ok(Async::NotReady) => return Ok(Async::NotReady),
            Err(err) => return Err((state.take().client, err.into())),
        }
        let state = state.take();
        transition!(Running {
            future: (state.future_closure)(state.client),
        })
    }

    fn poll_running<'a>(
        state: &'a mut RentToOwn<'a, Running<F, C, T, E>>,
    ) -> Poll<AfterRunning<C, T, E>, (C, E)> {
        match state.future.poll() {
            Ok(Async::NotReady) => return Ok(Async::NotReady),
            Ok(Async::Ready((mut client, t))) => transition!(Finishing {
                future: client.deref_mut2().0.batch_execute("COMMIT"),
                result: Ok(t),
                client,
            }),
            Err((mut client, e)) => transition!(Finishing {
                future: client.deref_mut2().0.batch_execute("ROLLBACK"),
                result: Err(e),
                client,
            }),
        }
    }

    fn poll_finishing<'a>(
        state: &'a mut RentToOwn<'a, Finishing<C, T, E>>,
    ) -> Poll<AfterFinishing<C, T>, (C, E)> {
        match state.future.poll() {
            Ok(Async::NotReady) => return Ok(Async::NotReady),
            Ok(Async::Ready(())) => {
                let state = state.take();

                match state.result {
                    Ok(res) => transition!(Finished((state.client, res))),
                    Err(err) => Err((state.client, err.into())),
                }
            }
            Err(e) => {
                let state = state.take();
                match state.result {
                    Ok(_) => Err((state.client, e.into())),
                    // prioritize the future's error over the rollback error
                    Err(e) => Err((state.client, e)),
                }
            }
        }
    }
}

impl<FC, F, C, T, E> Transaction2Future<FC, F, C, T, E>
where
    C: DerefMut2<Target = Client>,
    F: Future<Item = (C, T), Error = (C, E)>,
    FC: Fn(C) -> F,
    E: From<Error>,
{
    pub fn new(client: C, future_closure: FC) -> Transaction2Future<FC, F, C, T, E> {
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
