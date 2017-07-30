using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BatchExecutor.Extensions;

namespace BatchExecutor
{
	public class BatchExecutor<TItem, TResult> : IBatchExecutor<TItem, TResult>
	{
		private readonly ConcurrentQueue<WorkItem<TItem, TResult>> _itemsQueue = new ConcurrentQueue<WorkItem<TItem, TResult>>();
		private readonly int _batchSize;
		private readonly int _counterZeroingThreshold;
		private readonly bool _flushBufferOnDispose;
		private Timer _flushTimer;
		private readonly Func<IReadOnlyList<TItem>, Task<IDictionary<TItem, TResult>>> _batchExecutor;
		private int _counter;
		private readonly IObjectsPool<WorkItem<TItem, TResult>[]> _buffersPool = new ObjectsPool<WorkItem<TItem, TResult>[]>();
		private readonly IObjectsPool<TItem[]> _argumentsPool = new ObjectsPool<TItem[]>();
		private volatile bool _disposed;

		public BatchExecutor(int batchSize, Func<IReadOnlyList<TItem>, Task<IDictionary<TItem, TResult>>> batchExecutor, TimeSpan bufferFlushInterval, bool flushBufferOnDispose = false)
		{
			_batchSize = batchSize;
			_counterZeroingThreshold = int.MaxValue / _batchSize * _batchSize;
			_flushBufferOnDispose = flushBufferOnDispose;
			_batchExecutor = batchExecutor ?? throw new ArgumentNullException(nameof(batchExecutor));
			_flushTimer = new Timer(BufferFlushCallback, null, bufferFlushInterval, bufferFlushInterval);
		}

		public Task<TResult> ExecAsync(TItem item)
		{
			CheckDisposed();
			var tcs = new TaskCompletionSource<TResult>();
			ExecInternal(item, (exception, result) => ProcessResponse(tcs, exception, result));
			return tcs.Task;
		}

		private void ExecInternal(TItem dataItem, Action<Exception, TResult> callback)
		{
			EnqueItem(dataItem, callback);

			if (Interlocked.Increment(ref _counter) % _batchSize == 0)
				FlushBuffer(_batchSize);
		}

		private void EnqueItem(TItem dataItem, Action<Exception, TResult> callback)
		{
			var currentCounter = _counter;
			if (currentCounter == _counterZeroingThreshold)
				Interlocked.CompareExchange(ref _counter, 0, currentCounter);
			_itemsQueue.Enqueue(new WorkItem<TItem, TResult> { DataItem = dataItem, Callback = callback });
		}

		private void FlushBuffer(int flushSize)
		{
			var buffer = _buffersPool.GetOrCreate(() => new WorkItem<TItem, TResult>[_batchSize]);
			var i = 0;
			while (!_itemsQueue.IsEmpty && i < flushSize)
			{
				_itemsQueue.TryDequeue(out WorkItem<TItem, TResult> item);
				buffer[i] = item;
				i++;
			}
			ExecMulti(i, buffer);
		}

		private void ExecMulti(int bufferLength, WorkItem<TItem, TResult>[] buffer)
		{
			var arguments = _argumentsPool.GetOrCreate(() => new TItem[_batchSize]);
			for (var i = 0; i < bufferLength; i++)
			{
				arguments[i] = buffer[i].DataItem;
			}
			var argumentsSegment = new ArraySegment<TItem>(arguments, 0, bufferLength);
			_batchExecutor(argumentsSegment).ContinueWith(t =>
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
															 _argumentsPool.Release(arguments);
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
			var batchSize = _batchSize;
			var spinWait = new SpinWait();
			while (true)
			{
				var counter = _counter;
				var queueSize = counter % batchSize;
				if (queueSize == 0)
					return;

				var newCounter = counter - queueSize;
				if (Interlocked.CompareExchange(ref _counter, newCounter, counter) == counter)
				{
					FlushBuffer(queueSize);
					return;
				}
				spinWait.SpinOnce();
			}
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
					FlushBuffer(_batchSize);
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