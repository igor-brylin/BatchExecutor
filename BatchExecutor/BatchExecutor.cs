using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BatchExecutor.Extensions;

namespace BatchExecutor
{
	public class BatchExecutor<TItem, TResult> : IBatchExecutor<TItem, TResult>
	{
		private readonly ConcurrentQueue<WorkItem<TItem, TResult>> _itemsQueue = new ConcurrentQueue<WorkItem<TItem, TResult>>();
		private ManualResetEventSlim _resetEvent = new ManualResetEventSlim(true);
		private readonly int _batchSize;
		private readonly bool _flushBufferOnDispose;
		private Timer _flushTimer;
		private readonly Func<IList<TItem>, Task<IDictionary<TItem, TResult>>> _batchExecutor;
		private int _counter;
		private readonly IObjectsPool<WorkItem<TItem, TResult>[]> _buffersPool = new ObjectsPool<WorkItem<TItem, TResult>[]>();
		private volatile bool _disposed;

		public BatchExecutor(int batchSize, Func<IList<TItem>, Task<IDictionary<TItem, TResult>>> batchExecutor, TimeSpan bufferFlushInterval, bool flushBufferOnDispose = false)
		{
			_batchSize = batchSize;
			_flushBufferOnDispose = flushBufferOnDispose;
			_batchExecutor = batchExecutor ?? throw new ArgumentNullException(nameof(batchExecutor));
			_flushTimer = new Timer(BufferFlushCallback, null, bufferFlushInterval, bufferFlushInterval);
		}

		public Task<TResult> ExecAsync(TItem i)
		{
			CheckDisposed();
			var tcs = new TaskCompletionSource<TResult>();
			ExecInternal(i, (exception, result) => ProcessResponse(tcs, exception, result));
			return tcs.Task;
		}

		private void ExecInternal(TItem dataItem, Action<Exception, TResult> callback)
		{
			_resetEvent.Wait();
			_itemsQueue.Enqueue(new WorkItem<TItem, TResult> { DataItem = dataItem, Callback = callback });
			if (Interlocked.Increment(ref _counter) == _batchSize)
				FlushBuffer();
		}

		private void FlushBuffer()
		{
			_resetEvent.Reset();
			var buffer = _buffersPool.GetOrCreate(() => new WorkItem<TItem, TResult>[_batchSize]);
			var i = 0;
			while (_itemsQueue.TryDequeue(out WorkItem<TItem, TResult> item) && i < _batchSize)
			{
				buffer[i] = item;
				i++;
			}
			Interlocked.Exchange(ref _counter, 0);
			_resetEvent.Set();
			ExecMulti(i, buffer);
		}

		private void ExecMulti(int bufferLength, WorkItem<TItem, TResult>[] buffer)
		{
			var arguments = buffer.Take(bufferLength).Select(wi => wi.DataItem).ToList(); // TODO [Igor Brylin]: пул буферов для аргументов.
			_batchExecutor(arguments).ContinueWith(t =>
												   {
													   var faulted = t.Status == TaskStatus.Faulted;
													   for (var i = 0; i < bufferLength; i++)
													   {
														   var workItem = buffer[i];
														   if (faulted)
														   {
															   workItem.Callback(t.Exception.UnwrapAggregation(), default(TResult));
														   }
														   else if (!t.Result.TryGetValue(workItem.DataItem, out TResult result))
														   {
															   workItem.Callback(new KeyNotFoundException($"Record for {workItem.DataItem} not found."), default(TResult));
														   }
														   else
														   {
															   workItem.Callback(null, result);
														   }
													   }
													   _buffersPool.Release(buffer);
												   });
		}

		private static void ProcessResponse(TaskCompletionSource<TResult> tcs, Exception e, TResult result)
		{
			if (e != null)
			{
				tcs.SetException(e);
			}
			else
			{
				tcs.SetResult(result);
			}
		}

		private void BufferFlushCallback(object state)
		{
			if (!_itemsQueue.IsEmpty)
				FlushBuffer();
		}

		private void CheckDisposed()
		{
			if (_disposed)
				throw new ObjectDisposedException(GetType().FullName);
		}

		private void Dispose(bool disposing)
		{
			if (_disposed)
				return;

			if (disposing)
			{
				_flushTimer.Dispose();
				_flushTimer = null;

				if (_flushBufferOnDispose && !_itemsQueue.IsEmpty)
					FlushBuffer();

				_resetEvent.Dispose();
				_resetEvent = null;
			}
			_disposed = true;
		}

		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}
	}
}
