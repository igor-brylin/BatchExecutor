using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace BatchExecutor.Tests
{
	[TestClass]
	public class BatchExecutorTest
	{
		[TestMethod]
		public async Task ExecAsync_ActionTrowsException_ExceptionCatchedByAwait()
		{
			using (var batchExecutor = new BatchExecutor<int, string>(5, async items =>
																		 {
																			 await Task.Delay(10);
																			 throw new Exception("Exception in action");
																		 }, TimeSpan.FromMilliseconds(50)))
			{
				var catched = false;
				try
				{
					await batchExecutor.ExecAsync(1).ConfigureAwait(false);
				}
				catch (Exception)
				{
					catched = true;
				}
				Assert.AreEqual(true, catched);
			}
		}

		[TestMethod]
		public async Task ExecAsync_ProcessQueue_QueueProcessedInCorrectOrder()
		{
			using (var batchExecutor = new BatchExecutor<int, string>(5, async items =>
																		 {
																			 await Task.Delay(10);
																			 var dictionary = items.ToDictionary(i => i, i => i.ToString());
																			 return dictionary;
																		 }, TimeSpan.FromMilliseconds(50)))
			{
				var errorsOccurs = false;
				var tasks = new List<Task<string>>();
				try
				{
					const int maxSteps = 17;
					for (var i = 0; i < maxSteps; i++)
					{
						tasks.Add(batchExecutor.ExecAsync(i));
					}
					await Task.WhenAll(tasks).ConfigureAwait(false);
					for (var i = 0; i < maxSteps; i++)
					{
						Assert.AreEqual(i.ToString(), tasks[i].Result);
					}
				}
				catch (Exception)
				{
					errorsOccurs = true;
				}
				Assert.AreEqual(false, errorsOccurs);
			}
		}

		[TestMethod]
		public async Task ExecAsync_BufferNotFull_ProcessedByTimer()
		{
			var flushInterval = TimeSpan.FromMilliseconds(100);
			using (var batchExecutor = new BatchExecutor<int, string>(500, async items =>
																		   {
																			   await Task.Delay(1);
																			   var dictionary = items.ToDictionary(i => i, i => i.ToString());
																			   return dictionary;
																		   }, flushInterval))
			{

				var sw = Stopwatch.StartNew();
				var result = await batchExecutor.ExecAsync(1).ConfigureAwait(false);
				sw.Stop();
				Console.WriteLine($"Timer: {flushInterval}, elapsed: {sw.Elapsed}");
				Assert.AreEqual("1", result);
				var deviation = flushInterval.TotalMilliseconds / 100 * 50;
				Assert.IsTrue(sw.Elapsed.TotalMilliseconds <= flushInterval.TotalMilliseconds + deviation);
			}
		}

		[TestMethod]
		public async Task ExecAsync_BufferFull_ProcessedImmediately()
		{
			var flushInterval = TimeSpan.FromMilliseconds(10000);
			var batchSize = 50;
			using (var batchExecutor = new BatchExecutor<int, string>(batchSize, async items =>
																		   {
																			   await Task.Delay(1);
																			   var dictionary = items.ToDictionary(i => i, i => i.ToString());
																			   return dictionary;
																		   }, flushInterval))
			{
				var sw = Stopwatch.StartNew();
				var tasks = new List<Task>();
				for (var i = 0; i < batchSize; i++)
				{
					tasks.Add(batchExecutor.ExecAsync(i));
				}
				await Task.WhenAll(tasks).ConfigureAwait(false);
				sw.Stop();
				Console.WriteLine($"Timer: {flushInterval}, elapsed: {sw.Elapsed}");
				var deviation = 50 / 100 * 50;
				Assert.IsTrue(sw.Elapsed.TotalMilliseconds <= flushInterval.TotalMilliseconds + deviation);
			}
		}

		[TestMethod]
		public async Task ExecAsync_ActionTrowsException_AllTasksInBatchHasSameException()
		{
			using (var batchExecutor = new BatchExecutor<int, string>(5, async items =>
																		 {
																			 await Task.Delay(1);
																			 var dictionary = items.ToDictionary(i => 411, i => i.ToString());
																			 return dictionary;
																		 }, TimeSpan.FromMilliseconds(50)))
			{
				var tasks = new List<Task>();
				for (var i = 0; i < 7; i++)
				{
					tasks.Add(batchExecutor.ExecAsync(i));
				}
				var catched = false;
				try
				{
					await Task.WhenAll(tasks).ConfigureAwait(false);
				}
				catch (Exception)
				{
					catched = true;
				}
				Assert.AreEqual(true, catched);
				Task tmp = null;
				foreach (var task in tasks)
				{
					if (tmp == null)
						tmp = task;
					else
					{
						Debug.Assert(tmp.Exception != null, "tmp.Exception != null");
						Debug.Assert(task.Exception != null, "task.Exception != null");
						Assert.AreEqual(tmp.Exception.Message, task.Exception.Message);
					}
				}
			}
		}

		[TestMethod]
		public async Task ExecAsync_LoadTesting_OK()
		{
			var batchExecutor = new BatchExecutor<int, string>(5, async items =>
																  {
																	  await Task.Delay(1);
																	  var dictionary = items.ToDictionary(i => i, i => i.ToString());
																	  return dictionary;
																  }, TimeSpan.FromMilliseconds(50));
			var tasks = new List<Task>();
			const int loopCount = 1000;
			for (var i = 1; i <= loopCount; i++)
			{
				var i1 = i;
				tasks.Add(Task.Run(async () =>
								   {
									   var result = await batchExecutor.ExecAsync(i1).ConfigureAwait(false);
									   Assert.AreEqual(i1.ToString(), result);
									   return result;
								   }));
			}
			await Task.WhenAll(tasks).ConfigureAwait(false);
		}

		[TestMethod]
		public async Task Dispose_QueueNotFull_FlushedSuccessfully()
		{
			var batchExecutor = new BatchExecutor<int, string>(50, async items =>
																  {
																	  await Task.Delay(10);
																	  var dictionary = items.ToDictionary(i => i, i => i.ToString());
																	  return dictionary;
																  }, TimeSpan.FromHours(1));
			var tasks = new List<Task<string>>();
			const int loopCount = 11;
			for (var i = 1; i <= loopCount; i++)
			{
				tasks.Add(batchExecutor.ExecAsync(i));
			}
			EnsureTaskStatus(loopCount, tasks, TaskStatus.WaitingForActivation, false);
			batchExecutor.Dispose();
			await Task.WhenAll(tasks).ConfigureAwait(false);
			EnsureTaskStatus(loopCount, tasks, TaskStatus.RanToCompletion, true);
		}

		private static void EnsureTaskStatus(int loopCount, List<Task<string>> tasks, TaskStatus status, bool ensureResult)
		{
			for (int i = 0; i < loopCount; i++)
			{
				var task = tasks[i];
				Assert.AreEqual(status, task.Status);
				if(ensureResult)
					Assert.AreEqual((i + 1).ToString(), task.Result);
			}
		}

		[TestMethod]
		public async Task ExecAsync_ManyTasks_AllCompletedSuccessfully()
		{
			var batchExecutor = new BatchExecutor<int, string>(157, ExecOnExternalStorageAsync, TimeSpan.FromMilliseconds(51));
			var tasks = new List<Task>();
			const int loopCount = 1800053;
			var startCounter = 0;
			var finishCounter = 0;
			var sw = Stopwatch.StartNew();
			try
			{
				for (var i = 1; i <= loopCount; i++)
				{
					//await ExecOnExternalStorageAsync(items);
					var i1 = i;
					tasks.Add(Task.Run(async () =>
									   {
										   Interlocked.Increment(ref startCounter);
										   try
										   {
											   var result = await batchExecutor.ExecAsync(i1).ConfigureAwait(false);
											   return result;
										   }
										   catch (Exception e)
										   {
											   Console.WriteLine(e);
											   throw;
										   }
										   finally
										   {
											   Interlocked.Increment(ref finishCounter);
										   }

									   }));
				}
				await Task.WhenAll(tasks).ConfigureAwait(false);

			}
			catch (Exception e)
			{
				Console.WriteLine(e);
				throw;
			}
			sw.Stop();
			Console.WriteLine("Elapsed: {0}", sw.Elapsed);
			Assert.AreEqual(startCounter, finishCounter);
		}

		private static async Task<IDictionary<int, string>> ExecOnExternalStorageAsync(IReadOnlyList<int> items)
		{
			await Task.Delay(158);
			var dictionary = items.ToDictionary(i => i, i => "Result for " + i);
			return dictionary;
		}
	}
}
