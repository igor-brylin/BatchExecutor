/*
   Copyright 2017 Igor Brylin

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
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
		private Timer _flushTimer;
		private readonly Func<IReadOnlyList<TItem>, Task<IDictionary<TItem, TResult>>> _batchExecutor;
		private int _counter;
		private readonly ObjectPool<WorkItem<TItem, TResult>[]> _buffersPool = new ObjectPool<WorkItem<TItem, TResult>[]>();
		private readonly ObjectPool<TItem[]> _argumentsPool = new ObjectPool<TItem[]>();
		private int _disposed;

		public BatchExecutor(int batchSize, Func<IReadOnlyList<TItem>, Task<IDictionary<TItem, TResult>>> batchExecutor, TimeSpan bufferFlushInterval)
		{
			_batchSize = batchSize;
			_counterZeroingThreshold = int.MaxValue / _batchSize * _batchSize;
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
			EnqueueItem(dataItem, callback);

			if (Interlocked.Increment(ref _counter) % _batchSize == 0)
				FlushBuffer(_batchSize);
		}

		private void EnqueueItem(TItem dataItem, Action<Exception, TResult> callback)
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
				// NOTE: don't move _itemsQueue.TryDequeue into while loop condition instead of _itemsQueue.IsEmpty. Otherwise, program may hangs.
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
																	 workItem.Callback(t.Exception.UnwrapAggregateException(), default(TResult));
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
			if (_disposed == 1)
				throw new ObjectDisposedException(GetType().FullName);
		}

		private void Dispose(bool disposing)
		{
			if (Interlocked.CompareExchange(ref _disposed, 1, 0) == 1)
				return;

			if (disposing)
			{
				_flushTimer.Dispose();
				_flushTimer = null;
				BufferFlushCallback(null);
			}
		}

		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}
	}
}