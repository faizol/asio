//
// detail/impl/io_uring_reactor.ipp
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//
// Copyright (c) 2003-2020 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#ifndef ASIO_DETAIL_IMPL_IO_URING_REACTOR_IPP
#define ASIO_DETAIL_IMPL_IO_URING_REACTOR_IPP

#if defined(_MSC_VER) && (_MSC_VER >= 1200)
# pragma once
#endif // defined(_MSC_VER) && (_MSC_VER >= 1200)

#include "asio/detail/config.hpp"

#if defined(ASIO_HAS_IO_URING)

#include <cstddef>
#include "asio/detail/io_uring_reactor.hpp"
#include "asio/detail/throw_error.hpp"
#include "asio/error.hpp"

#include "asio/detail/push_options.hpp"

namespace asio {
namespace detail {

io_uring_reactor::io_uring_reactor(asio::execution_context& ctx)
  : execution_context_service_base<io_uring_reactor>(ctx),
    scheduler_(use_service<scheduler>(ctx)),
    mutex_(ASIO_CONCURRENCY_HINT_IS_LOCKING(
          REACTOR_REGISTRATION, scheduler_.concurrency_hint())),
    shutdown_(false),
    registered_descriptors_mutex_(mutex_.enabled())
{
  int result = ::io_uring_queue_init(ring_size, &ring_, 0);
  if (result < 0)
    fail(result, "io_uring_queue_init");

  result = ::io_uring_ring_dontfork(&ring_);
  if (result < 0)
    fail(result, "io_uring_ring_dontfork");
}

io_uring_reactor::~io_uring_reactor()
{
  ::io_uring_queue_exit(&ring_);
}

void io_uring_reactor::shutdown()
{
  mutex::scoped_lock lock(mutex_);
  shutdown_ = true;
  lock.unlock();

  op_queue<operation> ops;

  while (descriptor_state* state = registered_descriptors_.first())
  {
    for (int i = 0; i < max_ops; ++i)
      ops.push(state->op_queue_[i]);
    state->shutdown_ = true;
    registered_descriptors_.free(state);
  }

  timer_queues_.get_all_timers(ops);

  scheduler_.abandon_operations(ops);
}

void io_uring_reactor::notify_fork(
    asio::execution_context::fork_event fork_ev)
{
}

void io_uring_reactor::init_task()
{
  scheduler_.init_task();
}

int io_uring_reactor::register_descriptor(socket_type descriptor,
    io_uring_reactor::per_descriptor_data& descriptor_data)
{
  descriptor_data = allocate_descriptor_state();

  ASIO_HANDLER_REACTOR_REGISTRATION((
        context(), static_cast<uintmax_t>(descriptor),
        reinterpret_cast<uintmax_t>(descriptor_data)));

  {
    mutex::scoped_lock descriptor_lock(descriptor_data->mutex_);

    descriptor_data->reactor_ = this;
    descriptor_data->descriptor_ = descriptor;
    descriptor_data->shutdown_ = false;
    for (int i = 0; i < max_ops; ++i)
      descriptor_data->try_speculative_[i] = true;
  }

  return 0;
}

int io_uring_reactor::register_internal_descriptor(
    int op_type, socket_type descriptor,
    io_uring_reactor::per_descriptor_data& descriptor_data, reactor_op* op)
{
  descriptor_data = allocate_descriptor_state();

  ASIO_HANDLER_REACTOR_REGISTRATION((
        context(), static_cast<uintmax_t>(descriptor),
        reinterpret_cast<uintmax_t>(descriptor_data)));

  {
    mutex::scoped_lock descriptor_lock(descriptor_data->mutex_);

    descriptor_data->reactor_ = this;
    descriptor_data->descriptor_ = descriptor;
    descriptor_data->shutdown_ = false;
    descriptor_data->op_queue_[op_type].push(op);
    for (int i = 0; i < max_ops; ++i)
      descriptor_data->try_speculative_[i] = true;
  }

  return 0;
}

void io_uring_reactor::move_descriptor(socket_type,
    io_uring_reactor::per_descriptor_data& target_descriptor_data,
    io_uring_reactor::per_descriptor_data& source_descriptor_data)
{
  target_descriptor_data = source_descriptor_data;
  source_descriptor_data = 0;
}

void io_uring_reactor::start_op(int op_type, socket_type descriptor,
    io_uring_reactor::per_descriptor_data& descriptor_data, reactor_op* op,
    bool is_continuation, bool allow_speculative)
{
  if (!descriptor_data)
  {
    op->ec_ = asio::error::bad_descriptor;
    post_immediate_completion(op, is_continuation);
    return;
  }

  mutex::scoped_lock descriptor_lock(descriptor_data->mutex_);

  if (descriptor_data->shutdown_)
  {
    post_immediate_completion(op, is_continuation);
    return;
  }

}

void io_uring_reactor::cancel_ops(socket_type,
    io_uring_reactor::per_descriptor_data& descriptor_data)
{
  if (!descriptor_data)
    return;

  mutex::scoped_lock descriptor_lock(descriptor_data->mutex_);

  op_queue<operation> ops;
  for (int i = 0; i < max_ops; ++i)
  {
    while (reactor_op* op = descriptor_data->op_queue_[i].front())
    {
      op->ec_ = asio::error::operation_aborted;
      descriptor_data->op_queue_[i].pop();
      ops.push(op);
    }
  }

  descriptor_lock.unlock();

  scheduler_.post_deferred_completions(ops);
}

void io_uring_reactor::deregister_descriptor(socket_type descriptor,
    io_uring_reactor::per_descriptor_data& descriptor_data, bool closing)
{
  if (!descriptor_data)
    return;

  mutex::scoped_lock descriptor_lock(descriptor_data->mutex_);

  if (!descriptor_data->shutdown_)
  {
    ASIO_HANDLER_REACTOR_DEREGISTRATION((
          context(), static_cast<uintmax_t>(descriptor),
          reinterpret_cast<uintmax_t>(descriptor_data)));

    // Leave descriptor_data set so that it will be freed by the subsequent
    // call to cleanup_descriptor_data.
  }
  else
  {
    // We are shutting down, so prevent cleanup_descriptor_data from freeing
    // the descriptor_data object and let the destructor free it instead.
    descriptor_data = 0;
  }
}

void io_uring_reactor::deregister_internal_descriptor(socket_type descriptor,
    io_uring_reactor::per_descriptor_data& descriptor_data)
{
  if (!descriptor_data)
    return;

  mutex::scoped_lock descriptor_lock(descriptor_data->mutex_);

  if (!descriptor_data->shutdown_)
  {
    ASIO_HANDLER_REACTOR_DEREGISTRATION((
          context(), static_cast<uintmax_t>(descriptor),
          reinterpret_cast<uintmax_t>(descriptor_data)));

    // Leave descriptor_data set so that it will be freed by the subsequent
    // call to cleanup_descriptor_data.
  }
  else
  {
    // We are shutting down, so prevent cleanup_descriptor_data from freeing
    // the descriptor_data object and let the destructor free it instead.
    descriptor_data = 0;
  }
}

void io_uring_reactor::cleanup_descriptor_data(
    per_descriptor_data& descriptor_data)
{
  if (descriptor_data)
  {
    free_descriptor_state(descriptor_data);
    descriptor_data = 0;
  }
}

void io_uring_reactor::run(long usec, op_queue<operation>& ops)
{
  // This code relies on the fact that the scheduler queues the reactor task
  // behind all descriptor operations generated by this function. This means,
  // that by the time we reach this point, any previously returned descriptor
  // operations have already been dequeued. Therefore it is now safe for us to
  // reuse and return them for the scheduler to queue again.

  if (usec > 0)
  {
    mutex::scoped_lock lock(mutex_);
    __kernel_timespec ts;
    ts.tv_sec = usec / 1000000;
    ts.tv_nsec = (usec % 1000000) * 1000;
    ::io_uring_sqe& sqe = get_sqe();
    ::io_uring_prep_timeout(&sqe, &ts, 0, 0);
    ::io_uring_sqe_set_data(&sqe, &timer_queues_);
    submit();
  }

  ::io_uring_cqe* cqe = 0;
  int result = (usec == 0)
    ? ::io_uring_peek_cqe(&ring_, &cqe)
    : ::io_uring_wait_cqe(&ring_, &cqe);

  bool check_timers = false;
  while (result == 0)
  {
    void* data = ::io_uring_cqe_get_data(cqe);
    if (data == this)
      break;
    check_timers = (data == &timer_queues_);
    ::io_uring_cqe_seen(&ring_, cqe);
    result = ::io_uring_peek_cqe(&ring_, &cqe);
  }

  if (check_timers)
  {
    mutex::scoped_lock lock(mutex_);
    timer_queues_.get_ready_timers(ops);
    update_timeout();
  }
}

void io_uring_reactor::interrupt()
{
  mutex::scoped_lock lock(mutex_);
  ::io_uring_sqe& sqe = get_sqe();
  ::io_uring_prep_nop(&sqe);
  ::io_uring_sqe_set_data(&sqe, this);
  submit();
}

io_uring_reactor::descriptor_state*
io_uring_reactor::allocate_descriptor_state()
{
  mutex::scoped_lock descriptors_lock(registered_descriptors_mutex_);
  return registered_descriptors_.alloc(ASIO_CONCURRENCY_HINT_IS_LOCKING(
        REACTOR_IO, scheduler_.concurrency_hint()));
}

void io_uring_reactor::free_descriptor_state(
    io_uring_reactor::descriptor_state* s)
{
  mutex::scoped_lock descriptors_lock(registered_descriptors_mutex_);
  registered_descriptors_.free(s);
}

void io_uring_reactor::do_add_timer_queue(timer_queue_base& queue)
{
  mutex::scoped_lock lock(mutex_);
  timer_queues_.insert(&queue);
}

void io_uring_reactor::do_remove_timer_queue(timer_queue_base& queue)
{
  mutex::scoped_lock lock(mutex_);
  timer_queues_.erase(&queue);
}

void io_uring_reactor::update_timeout()
{
  __kernel_timespec ts = get_timeout();
  ::io_uring_sqe& sqe = get_sqe();
  ::io_uring_prep_timeout(&sqe, &ts, 0, 0);
  ::io_uring_sqe_set_data(&sqe, &timer_queues_);
  submit();
}

__kernel_timespec io_uring_reactor::get_timeout() const
{
  __kernel_timespec ts;
  long usec = timer_queues_.wait_duration_usec(5 * 60 * 1000 * 1000);
  ts.tv_sec = usec / 1000000;
  ts.tv_nsec = usec ? (usec % 1000000) * 1000 : 1;
  return ts;
}

::io_uring_sqe& io_uring_reactor::get_sqe()
{
  ::io_uring_sqe* sqe = ::io_uring_get_sqe(&ring_);
  if (sqe)
    return *sqe;
  submit();
  sqe = ::io_uring_get_sqe(&ring_);
  if (!sqe)
    fail(-EAGAIN, "io_uring_get_sqe");
  return *sqe;
}

void io_uring_reactor::submit()
{
  int result = ::io_uring_submit(&ring_);
  if (result < 0)
    fail(result, "io_uring_submit");
}

void io_uring_reactor::fail(int result, const char* what)
{
  asio::error_code ec(-result,
      asio::error::get_system_category());
  asio::detail::throw_error(ec, what);
}

struct io_uring_reactor::perform_io_cleanup_on_block_exit
{
  explicit perform_io_cleanup_on_block_exit(io_uring_reactor* r)
    : reactor_(r), first_op_(0)
  {
  }

  ~perform_io_cleanup_on_block_exit()
  {
    if (first_op_)
    {
      // Post the remaining completed operations for invocation.
      if (!ops_.empty())
        reactor_->scheduler_.post_deferred_completions(ops_);

      // A user-initiated operation has completed, but there's no need to
      // explicitly call work_finished() here. Instead, we'll take advantage of
      // the fact that the scheduler will call work_finished() once we return.
    }
    else
    {
      // No user-initiated operations have completed, so we need to compensate
      // for the work_finished() call that the scheduler will make once this
      // operation returns.
      reactor_->scheduler_.compensating_work_started();
    }
  }

  io_uring_reactor* reactor_;
  op_queue<operation> ops_;
  operation* first_op_;
};

io_uring_reactor::descriptor_state::descriptor_state(bool locking)
  : operation(&io_uring_reactor::descriptor_state::do_complete),
    mutex_(locking)
{
}

operation* io_uring_reactor::descriptor_state::perform_io(uint32_t events)
{
  mutex_.lock();
  perform_io_cleanup_on_block_exit io_cleanup(reactor_);
  mutex::scoped_lock descriptor_lock(mutex_, mutex::scoped_lock::adopt_lock);

  // Exception operations must be processed first to ensure that any
  // out-of-band data is read before normal data.
  static const int flag[max_ops] = { POLLIN, POLLOUT, POLLPRI };
  for (int j = max_ops - 1; j >= 0; --j)
  {
    if (events & (flag[j] | POLLERR | POLLHUP))
    {
      try_speculative_[j] = true;
      while (reactor_op* op = op_queue_[j].front())
      {
        if (reactor_op::status status = op->perform())
        {
          op_queue_[j].pop();
          io_cleanup.ops_.push(op);
          if (status == reactor_op::done_and_exhausted)
          {
            try_speculative_[j] = false;
            break;
          }
        }
        else
          break;
      }
    }
  }

  // The first operation will be returned for completion now. The others will
  // be posted for later by the io_cleanup object's destructor.
  io_cleanup.first_op_ = io_cleanup.ops_.front();
  io_cleanup.ops_.pop();
  return io_cleanup.first_op_;
}

void io_uring_reactor::descriptor_state::do_complete(
    void* owner, operation* base,
    const asio::error_code& ec, std::size_t bytes_transferred)
{
  if (owner)
  {
    descriptor_state* descriptor_data = static_cast<descriptor_state*>(base);
    uint32_t events = static_cast<uint32_t>(bytes_transferred);
    if (operation* op = descriptor_data->perform_io(events))
    {
      op->complete(owner, ec, 0);
    }
  }
}

} // namespace detail
} // namespace asio

#include "asio/detail/pop_options.hpp"

#endif // defined(ASIO_HAS_IO_URING)

#endif // ASIO_DETAIL_IMPL_IO_URING_REACTOR_IPP
